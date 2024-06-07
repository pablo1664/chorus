package policy

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestBucketID_String(t *testing.T) {
	type fields struct {
		Account string
		Bucket  string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "empty",
			fields: fields{
				Account: "",
				Bucket:  "",
			},
			want: ":",
		},
		{
			name: "account and bucket",
			fields: fields{
				Account: "a",
				Bucket:  "b",
			},
			want: "a:b",
		},
		{
			name: "account",
			fields: fields{
				Account: "a",
				Bucket:  "",
			},
			want: "a:",
		},
		{
			name: "bucket",
			fields: fields{
				Account: "",
				Bucket:  "b",
			},
			want: ":b",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := BucketID{
				Account: tt.fields.Account,
				Bucket:  tt.fields.Bucket,
			}
			if got := b.String(); got != tt.want {
				t.Errorf("BucketID.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStorageBucketID_String(t *testing.T) {
	type fields struct {
		Storage  string
		BucketID BucketID
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "empty",
			fields: fields{
				Storage: "",
				BucketID: BucketID{
					Account: "",
					Bucket:  "",
				},
			},
			want: "::",
		},
		{
			name: "storage account bucket",
			fields: fields{
				Storage: "s",
				BucketID: BucketID{
					Account: "a",
					Bucket:  "b",
				},
			},
			want: "s:a:b",
		},
		{
			name: "no storage",
			fields: fields{
				Storage: "",
				BucketID: BucketID{
					Account: "a",
					Bucket:  "b",
				},
			},
			want: ":a:b",
		},
		{
			name: "no account",
			fields: fields{
				Storage: "s",
				BucketID: BucketID{
					Account: "",
					Bucket:  "b",
				},
			},
			want: "s::b",
		},
		{
			name: "no bucket",
			fields: fields{
				Storage: "s",
				BucketID: BucketID{
					Account: "a",
					Bucket:  "",
				},
			},
			want: "s:a:",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := StorageBucketID{
				Storage:  tt.fields.Storage,
				BucketID: tt.fields.BucketID,
			}
			if got := b.String(); got != tt.want {
				t.Errorf("StorageBucketID.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_newBucketID(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want BucketID
	}{
		{
			name: "account and bucket",
			args: args{
				s: "a:b",
			},
			want: BucketID{
				Account: "a",
				Bucket:  "b",
			},
		},
		{
			name: "account",
			args: args{
				s: "a:",
			},
			want: BucketID{
				Account: "a",
				Bucket:  "",
			},
		},
		{
			name: "bucket",
			args: args{
				s: ":b",
			},
			want: BucketID{
				Account: "",
				Bucket:  "b",
			},
		},
		{
			name: "empty",
			args: args{
				s: "",
			},
			want: BucketID{
				Account: "",
				Bucket:  "",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newBucketID(tt.args.s); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newBucketID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBucketID_bucketRoutingPolicyID(t *testing.T) {
	type fields struct {
		Account string
		Bucket  string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "account and bucket",
			fields: fields{
				Account: "a",
				Bucket:  "b",
			},
			want: "p:route:a:b",
		},
		{
			name: "empty",
			fields: fields{
				Account: "",
				Bucket:  "",
			},
			want: "p:route::",
		},
		{
			name: "account",
			fields: fields{
				Account: "a",
				Bucket:  "",
			},
			want: "p:route:a:",
		},
		{
			name: "bucket",
			fields: fields{
				Account: "",
				Bucket:  "b",
			},
			want: "p:route::b",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := BucketID{
				Account: tt.fields.Account,
				Bucket:  tt.fields.Bucket,
			}
			if got := b.bucketRoutingPolicyID(); got != tt.want {
				t.Errorf("BucketID.bucketRoutingPolicyID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBucketID_accountRoutingPolicyID(t *testing.T) {
	type fields struct {
		Account string
		Bucket  string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "account and bucket",
			fields: fields{
				Account: "a",
				Bucket:  "b",
			},
			want: "p:route:a:",
		},
		{
			name: "empty",
			fields: fields{
				Account: "",
				Bucket:  "",
			},
			want: "p:route::",
		},
		{
			name: "account",
			fields: fields{
				Account: "a",
				Bucket:  "",
			},
			want: "p:route:a:",
		},
		{
			name: "bucket",
			fields: fields{
				Account: "",
				Bucket:  "b",
			},
			want: "p:route::",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := BucketID{
				Account: tt.fields.Account,
				Bucket:  tt.fields.Bucket,
			}
			if got := b.accountRoutingPolicyID(); got != tt.want {
				t.Errorf("BucketID.accountRoutingPolicyID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStorageBucketID_storageReplID(t *testing.T) {
	type fields struct {
		Storage  string
		BucketID BucketID
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "stor acc buck",
			fields: fields{
				Storage: "s",
				BucketID: BucketID{
					Account: "a",
					Bucket:  "b",
				},
			},
			want: "p:repl:s::",
		},
		{
			name: "acc buck",
			fields: fields{
				Storage: "",
				BucketID: BucketID{
					Account: "a",
					Bucket:  "b",
				},
			},
			want: "p:repl:::",
		},
		{
			name: "buck",
			fields: fields{
				Storage: "",
				BucketID: BucketID{
					Account: "",
					Bucket:  "b",
				},
			},
			want: "p:repl:::",
		},
		{
			name: "empty",
			fields: fields{
				Storage: "",
				BucketID: BucketID{
					Account: "",
					Bucket:  "",
				},
			},
			want: "p:repl:::",
		},
		{
			name: "stor",
			fields: fields{
				Storage: "s",
				BucketID: BucketID{
					Account: "",
					Bucket:  "",
				},
			},
			want: "p:repl:s::",
		},
		{
			name: "stor acc",
			fields: fields{
				Storage: "s",
				BucketID: BucketID{
					Account: "a",
					Bucket:  "",
				},
			},
			want: "p:repl:s::",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := StorageBucketID{
				Storage:  tt.fields.Storage,
				BucketID: tt.fields.BucketID,
			}
			if got := b.storageReplID(); got != tt.want {
				t.Errorf("StorageBucketID.storageReplID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStorageBucketID_accountReplID(t *testing.T) {
	type fields struct {
		Storage  string
		BucketID BucketID
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "stor acc buck",
			fields: fields{
				Storage: "s",
				BucketID: BucketID{
					Account: "a",
					Bucket:  "b",
				},
			},
			want: "p:repl:s:a:",
		},
		{
			name: "acc buck",
			fields: fields{
				Storage: "",
				BucketID: BucketID{
					Account: "a",
					Bucket:  "b",
				},
			},
			want: "p:repl::a:",
		},
		{
			name: "buck",
			fields: fields{
				Storage: "",
				BucketID: BucketID{
					Account: "",
					Bucket:  "b",
				},
			},
			want: "p:repl:::",
		},
		{
			name: "empty",
			fields: fields{
				Storage: "",
				BucketID: BucketID{
					Account: "",
					Bucket:  "",
				},
			},
			want: "p:repl:::",
		},
		{
			name: "stor",
			fields: fields{
				Storage: "s",
				BucketID: BucketID{
					Account: "",
					Bucket:  "",
				},
			},
			want: "p:repl:s::",
		},
		{
			name: "stor acc",
			fields: fields{
				Storage: "s",
				BucketID: BucketID{
					Account: "a",
					Bucket:  "",
				},
			},
			want: "p:repl:s:a:",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := StorageBucketID{
				Storage:  tt.fields.Storage,
				BucketID: tt.fields.BucketID,
			}
			if got := b.accountReplID(); got != tt.want {
				t.Errorf("StorageBucketID.accountReplID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStorageBucketID_bucketReplID(t *testing.T) {
	type fields struct {
		Storage  string
		BucketID BucketID
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "stor acc buck",
			fields: fields{
				Storage: "s",
				BucketID: BucketID{
					Account: "a",
					Bucket:  "b",
				},
			},
			want: "p:repl:s:a:b",
		},
		{
			name: "acc buck",
			fields: fields{
				Storage: "",
				BucketID: BucketID{
					Account: "a",
					Bucket:  "b",
				},
			},
			want: "p:repl::a:b",
		},
		{
			name: "buck",
			fields: fields{
				Storage: "",
				BucketID: BucketID{
					Account: "",
					Bucket:  "b",
				},
			},
			want: "p:repl:::b",
		},
		{
			name: "empty",
			fields: fields{
				Storage: "",
				BucketID: BucketID{
					Account: "",
					Bucket:  "",
				},
			},
			want: "p:repl:::",
		},
		{
			name: "stor",
			fields: fields{
				Storage: "s",
				BucketID: BucketID{
					Account: "",
					Bucket:  "",
				},
			},
			want: "p:repl:s::",
		},
		{
			name: "stor acc",
			fields: fields{
				Storage: "s",
				BucketID: BucketID{
					Account: "a",
					Bucket:  "",
				},
			},
			want: "p:repl:s:a:",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := StorageBucketID{
				Storage:  tt.fields.Storage,
				BucketID: tt.fields.BucketID,
			}
			if got := b.bucketReplID(); got != tt.want {
				t.Errorf("StorageBucketID.bucketReplID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReplID_validate(t *testing.T) {
	type fields struct {
		Src  StorageBucketID
		Dest StorageBucketID
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "src invalid",
			fields: fields{
				Src: StorageBucketID{
					Storage: "s1:",
					BucketID: BucketID{
						Account: "a1",
						Bucket:  "b1",
					},
				},
				Dest: StorageBucketID{
					Storage: "s2",
					BucketID: BucketID{
						Account: "a2",
						Bucket:  "b2",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "dest invalid",
			fields: fields{
				Src: StorageBucketID{
					Storage: "s1",
					BucketID: BucketID{
						Account: "a1",
						Bucket:  "b1",
					},
				},
				Dest: StorageBucketID{
					Storage: "s2:",
					BucketID: BucketID{
						Account: "a2",
						Bucket:  "b2",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "only src buck is empty",
			fields: fields{
				Src: StorageBucketID{
					Storage: "s1",
					BucketID: BucketID{
						Account: "a1",
						Bucket:  "",
					},
				},
				Dest: StorageBucketID{
					Storage: "s2",
					BucketID: BucketID{
						Account: "a2",
						Bucket:  "b2",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "only dest buck is empty",
			fields: fields{
				Src: StorageBucketID{
					Storage: "s1",
					BucketID: BucketID{
						Account: "a1",
						Bucket:  "b1",
					},
				},
				Dest: StorageBucketID{
					Storage: "s2",
					BucketID: BucketID{
						Account: "a2",
						Bucket:  "",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "only src acc is empty",
			fields: fields{
				Src: StorageBucketID{
					Storage: "s1",
					BucketID: BucketID{
						Account: "",
						Bucket:  "b1",
					},
				},
				Dest: StorageBucketID{
					Storage: "s2",
					BucketID: BucketID{
						Account: "a2",
						Bucket:  "b2",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "only dest acc is empty",
			fields: fields{
				Src: StorageBucketID{
					Storage: "s1",
					BucketID: BucketID{
						Account: "a1",
						Bucket:  "b1",
					},
				},
				Dest: StorageBucketID{
					Storage: "s2",
					BucketID: BucketID{
						Account: "",
						Bucket:  "b2",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "both acc are empty",
			fields: fields{
				Src: StorageBucketID{
					Storage: "s1",
					BucketID: BucketID{
						Account: "",
						Bucket:  "b1",
					},
				},
				Dest: StorageBucketID{
					Storage: "s2",
					BucketID: BucketID{
						Account: "",
						Bucket:  "b2",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "both buckets are empty",
			fields: fields{
				Src: StorageBucketID{
					Storage: "s1",
					BucketID: BucketID{
						Account: "a1",
						Bucket:  "",
					},
				},
				Dest: StorageBucketID{
					Storage: "s2",
					BucketID: BucketID{
						Account: "a2",
						Bucket:  "",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "stor to stor",
			fields: fields{
				Src: StorageBucketID{
					Storage: "s1",
					BucketID: BucketID{
						Account: "",
						Bucket:  "",
					},
				},
				Dest: StorageBucketID{
					Storage: "s2",
					BucketID: BucketID{
						Account: "",
						Bucket:  "",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "cant sync to same stor",
			fields: fields{
				Src: StorageBucketID{
					Storage: "s1",
					BucketID: BucketID{
						Account: "",
						Bucket:  "",
					},
				},
				Dest: StorageBucketID{
					Storage: "s1",
					BucketID: BucketID{
						Account: "",
						Bucket:  "",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "cant sync to itself",
			fields: fields{
				Src: StorageBucketID{
					Storage: "s1",
					BucketID: BucketID{
						Account: "a1",
						Bucket:  "b1",
					},
				},
				Dest: StorageBucketID{
					Storage: "s1",
					BucketID: BucketID{
						Account: "a1",
						Bucket:  "b1",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "cant sync same acc",
			fields: fields{
				Src: StorageBucketID{
					Storage: "s1",
					BucketID: BucketID{
						Account: "a1",
						Bucket:  "",
					},
				},
				Dest: StorageBucketID{
					Storage: "s1",
					BucketID: BucketID{
						Account: "a1",
						Bucket:  "",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "cant sync same bucket",
			fields: fields{
				Src: StorageBucketID{
					Storage: "s1",
					BucketID: BucketID{
						Account: "",
						Bucket:  "b1",
					},
				},
				Dest: StorageBucketID{
					Storage: "s1",
					BucketID: BucketID{
						Account: "",
						Bucket:  "b1",
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := ReplID{
				Src:  tt.fields.Src,
				Dest: tt.fields.Dest,
			}
			if err := r.validate(); (err != nil) != tt.wantErr {
				t.Errorf("ReplID.validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBucketID_validate(t *testing.T) {
	type fields struct {
		Account string
		Bucket  string
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "ok",
			fields: fields{
				Account: "a",
				Bucket:  "b",
			},
			wantErr: false,
		},
		{
			name: "acc contains separator",
			fields: fields{
				Account: "a:",
				Bucket:  "b",
			},
			wantErr: true,
		},
		{
			name: "bucket contains separator",
			fields: fields{
				Account: "a",
				Bucket:  "b:",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := BucketID{
				Account: tt.fields.Account,
				Bucket:  tt.fields.Bucket,
			}
			if err := b.validate(); (err != nil) != tt.wantErr {
				t.Errorf("BucketID.validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStorageBucketID_validate(t *testing.T) {
	type fields struct {
		Storage  string
		BucketID BucketID
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "ok",
			fields: fields{
				Storage: "s",
				BucketID: BucketID{
					Account: "a",
					Bucket:  "b",
				},
			},
			wantErr: false,
		},
		{
			name: "storage contains separator",
			fields: fields{
				Storage: "s:",
				BucketID: BucketID{
					Account: "a",
					Bucket:  "b",
				},
			},
			wantErr: true,
		},
		{
			name: "account contains separator",
			fields: fields{
				Storage: "s",
				BucketID: BucketID{
					Account: "a:",
					Bucket:  "b",
				},
			},
			wantErr: true,
		},
		{
			name: "bucket contains separator",
			fields: fields{
				Storage: "s",
				BucketID: BucketID{
					Account: "a",
					Bucket:  "b:",
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := StorageBucketID{
				Storage:  tt.fields.Storage,
				BucketID: tt.fields.BucketID,
			}
			if err := b.validate(); (err != nil) != tt.wantErr {
				t.Errorf("StorageBucketID.validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_RoutingPolicy_e2e(t *testing.T) {
	r := require.New(t)
	db := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: db.Addr()})
	main, second, third, fourth := "main", "second", "third", "fourth"
	b1, b2, a1 := "b1", "b2", "a1"
	storages := map[string]bool{
		main:   true,
		second: false,
		third:  false,
		fourth: false,
	}
	ctx := context.TODO()

	s := NewSvc2(storages, c)

	route, err := s.GetRoutingPolicy(ctx, BucketID{})
	r.NoError(err)
	r.EqualValues(main, route, "fallback to main")

	r.ErrorIs(s.AddRoutingPolicy(ctx, BucketID{}, second), dom.ErrInvalidArg, "cannot override default routing")
	r.ErrorIs(s.AddRoutingPolicy(ctx, BucketID{Bucket: b1}, "asdf"), dom.ErrInvalidArg, "cannot route to unknown storage")
	r.NoError(s.AddRoutingPolicy(ctx, BucketID{Account: a1, Bucket: ""}, second), "create account routing")
	r.ErrorIs(s.AddRoutingPolicy(ctx, BucketID{Account: a1, Bucket: ""}, second), dom.ErrAlreadyExists, "cannot duplicate")
	r.NoError(s.AddRoutingPolicy(ctx, BucketID{Account: a1, Bucket: b1}, third), "create bucket routing")
	r.ErrorIs(s.AddRoutingPolicy(ctx, BucketID{Account: a1, Bucket: b1}, third), dom.ErrAlreadyExists, "cannot duplicate")
	r.NoError(s.AddRoutingPolicy(ctx, BucketID{Account: "", Bucket: b2}, fourth), "empty account allowed")
	r.ErrorIs(s.AddRoutingPolicy(ctx, BucketID{Account: "", Bucket: b2}, fourth), dom.ErrAlreadyExists, "cannot duplicate")

	route, err = s.GetRoutingPolicy(ctx, BucketID{})
	r.NoError(err)
	r.EqualValues(main, route, "fallback to main")

	route, err = s.GetRoutingPolicy(ctx, BucketID{
		Account: "unknown",
		Bucket:  "unknown",
	})
	r.NoError(err)
	r.EqualValues(main, route, "fallback to main")

	route, err = s.GetRoutingPolicy(ctx, BucketID{
		Account: "",
		Bucket:  b1,
	})
	r.NoError(err)
	r.EqualValues(main, route, "fallback to main")

	route, err = s.GetRoutingPolicy(ctx, BucketID{
		Account: "unknown",
		Bucket:  b1,
	})
	r.NoError(err)
	r.EqualValues(main, route, "fallback to main")

	route, err = s.GetRoutingPolicy(ctx, BucketID{
		Account: a1,
		Bucket:  "",
	})
	r.NoError(err)
	r.EqualValues(second, route, "fallback to account routing")

	route, err = s.GetRoutingPolicy(ctx, BucketID{
		Account: a1,
		Bucket:  "unknown",
	})
	r.NoError(err)
	r.EqualValues(second, route, "fallback to account routing")

	route, err = s.GetRoutingPolicy(ctx, BucketID{
		Account: a1,
		Bucket:  b1,
	})
	r.NoError(err)
	r.EqualValues(third, route, "got bucket routing")

	route, err = s.GetRoutingPolicy(ctx, BucketID{
		Account: "",
		Bucket:  b2,
	})
	r.NoError(err)
	r.EqualValues(fourth, route, "got bucket routing for empty account")

	r.ErrorIs(s.DeleteRoutingPolicy(ctx, BucketID{
		Account: "",
		Bucket:  "",
	}), dom.ErrNotFound, "cannot delete default routing")

	r.ErrorIs(s.DeleteRoutingPolicy(ctx, BucketID{
		Account: "unknown",
		Bucket:  "unknown",
	}), dom.ErrNotFound, "cannot delete non-existing routing")

	r.NoError(s.DeleteRoutingPolicy(ctx, BucketID{
		Account: a1,
		Bucket:  b1,
	}), "delete bucket policy")

	route, err = s.GetRoutingPolicy(ctx, BucketID{
		Account: a1,
		Bucket:  b1,
	})
	r.NoError(err)
	r.EqualValues(second, route, "fallback to account routing after delete")

	r.NoError(s.DeleteRoutingPolicy(ctx, BucketID{
		Account: a1,
		Bucket:  "",
	}), "delete account policy")

	route, err = s.GetRoutingPolicy(ctx, BucketID{
		Account: a1,
		Bucket:  b1,
	})
	r.NoError(err)
	r.EqualValues(main, route, "fallback to main routing after account delete")

	r.NoError(s.DeleteRoutingPolicy(ctx, BucketID{
		Account: "",
		Bucket:  b2,
	}), "delete empty account policy")
}

func Test_RoutingPolicyBlock_e2e(t *testing.T) {
	r := require.New(t)
	db := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: db.Addr()})
	main, second, third, fourth := "main", "second", "third", "fourth"
	b1, b2, a1, a2 := "b1", "b2", "a1", "a2"
	storages := map[string]bool{
		main:   true,
		second: false,
		third:  false,
		fourth: false,
	}
	ctx := context.TODO()

	s := NewSvc2(storages, c)

	route, err := s.GetRoutingPolicy(ctx, BucketID{})
	r.NoError(err)
	r.EqualValues(main, route, "fallback to main")

	route, err = s.GetRoutingPolicy(ctx, BucketID{
		Account: a1,
		Bucket:  b1,
	})
	r.NoError(err)
	r.EqualValues(main, route, "fallback to main")

	r.NoError(s.AddRoutingPolicy(ctx, BucketID{Account: a1, Bucket: b1}, second), "create bucket routing")
	r.ErrorIs(s.addRoutingBlockPolicy(ctx, BucketID{}), dom.ErrInvalidArg, "cannot block default")
	r.ErrorIs(s.addRoutingBlockPolicy(ctx, BucketID{Account: a1, Bucket: b1}), dom.ErrAlreadyExists, "cannot block existing")
	r.NoError(s.addRoutingBlockPolicy(ctx, BucketID{Account: "", Bucket: b1}), "block bucket with no account")
	r.NoError(s.addRoutingBlockPolicy(ctx, BucketID{Account: a2, Bucket: b2}), "block bucket with account")
	r.NoError(s.addRoutingBlockPolicy(ctx, BucketID{Account: a2, Bucket: b1}), "block bucket with account")
	r.NoError(s.addRoutingBlockPolicy(ctx, BucketID{Account: a1, Bucket: b2}), "block bucket with account")

	route, err = s.GetRoutingPolicy(ctx, BucketID{
		Account: a1,
		Bucket:  b1,
	})
	r.NoError(err)
	r.EqualValues(second, route, "bucket route")

	_, err = s.GetRoutingPolicy(ctx, BucketID{
		Account: "",
		Bucket:  b1,
	})
	r.ErrorIs(err, dom.ErrRoutingBlocked, "bucket with no account blocked")

	_, err = s.GetRoutingPolicy(ctx, BucketID{
		Account: a2,
		Bucket:  b2,
	})
	r.ErrorIs(err, dom.ErrRoutingBlocked, "bucket with account blocked")

	_, err = s.GetRoutingPolicy(ctx, BucketID{
		Account: a2,
		Bucket:  b1,
	})
	r.ErrorIs(err, dom.ErrRoutingBlocked, "bucket with account blocked")

	_, err = s.GetRoutingPolicy(ctx, BucketID{
		Account: a1,
		Bucket:  b2,
	})
	r.ErrorIs(err, dom.ErrRoutingBlocked, "bucket with account blocked")

	r.NoError(s.DeleteRoutingPolicy(ctx, BucketID{
		Account: a2,
		Bucket:  b2,
	}), "delete account policy")

	route, err = s.GetRoutingPolicy(ctx, BucketID{
		Account: a2,
		Bucket:  b2,
	})
	r.NoError(err)
	r.EqualValues(main, route, "bucket route after block delete")

	list, err := s.ListBlockedBuckets(ctx, "")
	r.NoError(err, "list for all accounts")
	r.Len(list, 2, "list for all accounts")
	r.Contains(list, b1, "list for all accounts")
	r.Contains(list, b2, "list for all accounts")

	list, err = s.ListBlockedBuckets(ctx, a1)
	r.NoError(err, "list for a1 account")
	r.Len(list, 1, "list for a1 account")
	r.Contains(list, b2, "list for a1 account")

	list, err = s.ListBlockedBuckets(ctx, a2)
	r.NoError(err, "list for a2 account")
	r.Len(list, 1, "list for a2 account")
	r.Contains(list, b1, "list for a2 account")

	list, err = s.ListBlockedBuckets(ctx, "unknown")
	r.NoError(err, "list for unknown account")
	r.Empty(list, "list for unknown account")
}

func Test_lua_luaZIncrByEx(t *testing.T) {
	r := require.New(t)
	db := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: db.Addr()})
	ctx := context.TODO()

	key := "key"
	member := "member"

	_, err := luaZIncrByEx.Run(ctx, c, []string{key}, member, 1.0).Result()
	r.ErrorIs(err, redis.Nil, "return redis.Nil if set and member not exists")
	r.ErrorIs(c.ZScore(ctx, key, member).Err(), redis.Nil, "set member was not created")

	r.NoError(c.ZAdd(ctx, key, redis.Z{
		Score:  68,
		Member: member,
	}).Err(), "add member to set")

	_, err = luaZIncrByEx.Run(ctx, c, []string{key}, "otherMember", 1.0).Result()
	r.ErrorIs(err, redis.Nil, "return redis.Nil if set exists and member not exists")
	r.ErrorIs(c.ZScore(ctx, key, "otherMember").Err(), redis.Nil, "set member was not created")

	res := luaZIncrByEx.Run(ctx, c, []string{key}, member, 1.0)
	r.NoError(res.Err(), "inc with no err")
	resFloat, err := res.Float64()
	r.NoError(err, "float returned")
	r.EqualValues(69., resFloat, "increased val returned")
	r.NoError(c.ZScore(ctx, key, member).Err(), "set member is created")
	r.EqualValues(69., c.ZScore(ctx, key, member).Val(), "set member is created")
}

func Test_policySvc2_AddReplicationPolicy(t *testing.T) {
	storages := map[string]bool{
		"one":   true,
		"two":   false,
		"three": false,
	}
	type existing struct {
		routing     []StorageBucketID
		replication []ReplID
	}
	type args struct {
		id ReplID
	}
	tests := []struct {
		name     string
		existing existing
		args     args
		wantErr  error
	}{
		//--------------------------OK: bucket level---------------------------------
		{
			name: "ok: bucket-level different storage, same bucket",
			existing: existing{
				routing:     []StorageBucketID{},
				replication: []ReplID{},
			},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b1",
						},
					},
					Dest: StorageBucketID{
						Storage: "two",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b1",
						},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "ok: bucket-level: different storage, different bucket",
			existing: existing{
				routing:     []StorageBucketID{},
				replication: []ReplID{},
			},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b1",
						},
					},
					Dest: StorageBucketID{
						Storage: "two",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b2",
						},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "ok: bucket-level: different storage, different account",
			existing: existing{
				routing:     []StorageBucketID{},
				replication: []ReplID{},
			},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b1",
						},
					},
					Dest: StorageBucketID{
						Storage: "two",
						BucketID: BucketID{
							Account: "a2",
							Bucket:  "b1",
						},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "ok: bucket-level: same storage, different account",
			existing: existing{
				routing:     []StorageBucketID{},
				replication: []ReplID{},
			},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b1",
						},
					},
					Dest: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a2",
							Bucket:  "b1",
						},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "ok: bucket-level: same storage, different bucket",
			existing: existing{
				routing:     []StorageBucketID{},
				replication: []ReplID{},
			},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b1",
						},
					},
					Dest: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b2",
						},
					},
				},
			},
			wantErr: nil,
		},

		//--------------------------OK: acc level---------------------------------
		{
			name: "ok: acc-level: different storage, same acc",
			existing: existing{
				routing:     []StorageBucketID{},
				replication: []ReplID{},
			},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "",
						},
					},
					Dest: StorageBucketID{
						Storage: "two",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "",
						},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "ok: acc-level: different storage, different acc",
			existing: existing{
				routing:     []StorageBucketID{},
				replication: []ReplID{},
			},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "",
						},
					},
					Dest: StorageBucketID{
						Storage: "two",
						BucketID: BucketID{
							Account: "a2",
							Bucket:  "",
						},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "ok: acc-level: same storage, different acc",
			existing: existing{
				routing:     []StorageBucketID{},
				replication: []ReplID{},
			},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "",
						},
					},
					Dest: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a2",
							Bucket:  "",
						},
					},
				},
			},
			wantErr: nil,
		},
		//--------------------------OK: storage level---------------------------------
		{
			name: "ok: storage-level: different storage",
			existing: existing{
				routing:     []StorageBucketID{},
				replication: []ReplID{},
			},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "",
							Bucket:  "",
						},
					},
					Dest: StorageBucketID{
						Storage: "two",
						BucketID: BucketID{
							Account: "",
							Bucket:  "",
						},
					},
				},
			},
			wantErr: nil,
		},
		//--------------------------OK: with existing data--------------------------------
		{
			name: "ok: same storage 2 buckets repl",
			existing: existing{
				routing: []StorageBucketID{},
				replication: []ReplID{
					{
						Src: StorageBucketID{
							Storage: "one",
							BucketID: BucketID{
								Account: "a1",
								Bucket:  "b1",
							},
						},
						Dest: StorageBucketID{
							Storage: "one",
							BucketID: BucketID{
								Account: "a1",
								Bucket:  "b2",
							},
						},
					},
				},
			},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b1",
						},
					},
					Dest: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b3",
						},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "ok: same storage 2 accs repl",
			existing: existing{
				routing: []StorageBucketID{},
				replication: []ReplID{
					{
						Src: StorageBucketID{
							Storage: "one",
							BucketID: BucketID{
								Account: "a1",
								Bucket:  "",
							},
						},
						Dest: StorageBucketID{
							Storage: "one",
							BucketID: BucketID{
								Account: "a2",
								Bucket:  "",
							},
						},
					},
				},
			},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "",
						},
					},
					Dest: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a3",
							Bucket:  "",
						},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "ok:same storage 2 accs repl",
			existing: existing{
				routing: []StorageBucketID{},
				replication: []ReplID{
					{
						Src: StorageBucketID{
							Storage: "one",
							BucketID: BucketID{
								Account: "a1",
								Bucket:  "",
							},
						},
						Dest: StorageBucketID{
							Storage: "one",
							BucketID: BucketID{
								Account: "a2",
								Bucket:  "",
							},
						},
					},
				},
			},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "",
						},
					},
					Dest: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a3",
							Bucket:  "",
						},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "ok: diff storages",
			existing: existing{
				routing: []StorageBucketID{},
				replication: []ReplID{
					{
						Src: StorageBucketID{
							Storage: "one",
							BucketID: BucketID{
								Account: "a1",
								Bucket:  "b1",
							},
						},
						Dest: StorageBucketID{
							Storage: "two",
							BucketID: BucketID{
								Account: "a1",
								Bucket:  "b1",
							},
						},
					},
				},
			},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b2",
						},
					},
					Dest: StorageBucketID{
						Storage: "two",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b2",
						},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "ok: diff accs",
			existing: existing{
				routing: []StorageBucketID{},
				replication: []ReplID{
					{
						Src: StorageBucketID{
							Storage: "one",
							BucketID: BucketID{
								Account: "a1",
								Bucket:  "",
							},
						},
						Dest: StorageBucketID{
							Storage: "two",
							BucketID: BucketID{
								Account: "a1",
								Bucket:  "",
							},
						},
					},
				},
			},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a2",
							Bucket:  "",
						},
					},
					Dest: StorageBucketID{
						Storage: "two",
						BucketID: BucketID{
							Account: "a2",
							Bucket:  "",
						},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "ok: 2 storages",
			existing: existing{
				routing: []StorageBucketID{},
				replication: []ReplID{
					{
						Src: StorageBucketID{
							Storage: "one",
							BucketID: BucketID{
								Account: "",
								Bucket:  "",
							},
						},
						Dest: StorageBucketID{
							Storage: "two",
							BucketID: BucketID{
								Account: "",
								Bucket:  "",
							},
						},
					},
				},
			},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "",
							Bucket:  "",
						},
					},
					Dest: StorageBucketID{
						Storage: "three",
						BucketID: BucketID{
							Account: "",
							Bucket:  "",
						},
					},
				},
			},
			wantErr: nil,
		},
		// ------------------------------ validation errors -----------------------
		{
			name:     "unknown src storage",
			existing: existing{},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "asdf",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b1",
						},
					},
					Dest: StorageBucketID{
						Storage: "two",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b1",
						},
					},
				},
			},
			wantErr: dom.ErrInvalidArg,
		},
		{
			name:     "unknown dest storage",
			existing: existing{},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b1",
						},
					},
					Dest: StorageBucketID{
						Storage: "asdf",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b1",
						},
					},
				},
			},
			wantErr: dom.ErrInvalidArg,
		},
		{
			name:     "cannot repl acc-level to bucket level",
			existing: existing{},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "",
						},
					},
					Dest: StorageBucketID{
						Storage: "two",
						BucketID: BucketID{
							Account: "a2",
							Bucket:  "b1",
						},
					},
				},
			},
			wantErr: dom.ErrInvalidArg,
		},
		{
			name:     "cannot repl bucket level to acc level",
			existing: existing{},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b1",
						},
					},
					Dest: StorageBucketID{
						Storage: "two",
						BucketID: BucketID{
							Account: "a2",
							Bucket:  "",
						},
					},
				},
			},
			wantErr: dom.ErrInvalidArg,
		},
		{
			name:     "src stor cannot be different from routing stor",
			existing: existing{},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "two",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b1",
						},
					},
					Dest: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b1",
						},
					},
				},
			},
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "cannot replicate if src is already used as dest",
			existing: existing{
				routing: []StorageBucketID{},
				replication: []ReplID{
					{
						Src: StorageBucketID{
							Storage: "one",
							BucketID: BucketID{
								Account: "a1",
								Bucket:  "b1",
							},
						},
						Dest: StorageBucketID{
							Storage: "one",
							BucketID: BucketID{
								Account: "a1",
								Bucket:  "b2",
							},
						},
					},
				},
			},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b2",
						},
					},
					Dest: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b3",
						},
					},
				},
			},
			wantErr: dom.ErrRoutingBlocked,
		},
		{
			name: "cannot replicate if src is already used as dest on acc lvl",
			existing: existing{
				routing: []StorageBucketID{},
				replication: []ReplID{
					{
						Src: StorageBucketID{
							Storage: "one",
							BucketID: BucketID{
								Account: "a1",
								Bucket:  "",
							},
						},
						Dest: StorageBucketID{
							Storage: "one",
							BucketID: BucketID{
								Account: "a2",
								Bucket:  "",
							},
						},
					},
				},
			},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a2",
							Bucket:  "b1",
						},
					},
					Dest: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b1",
						},
					},
				},
			},
			wantErr: dom.ErrRoutingBlocked,
		},
		{
			name: "cannot replicate if src acc is already used as dest on stor lvl",
			existing: existing{
				routing: []StorageBucketID{},
				replication: []ReplID{
					{
						Src: StorageBucketID{
							Storage: "one",
							BucketID: BucketID{
								Account: "a1",
								Bucket:  "",
							},
						},
						Dest: StorageBucketID{
							Storage: "one",
							BucketID: BucketID{
								Account: "a2",
								Bucket:  "",
							},
						},
					},
				},
			},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a2",
							Bucket:  "",
						},
					},
					Dest: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a3",
							Bucket:  "",
						},
					},
				},
			},
			wantErr: dom.ErrRoutingBlocked,
		},
		{
			name: "cannot replicate if dest is already used as src",
			existing: existing{
				routing: []StorageBucketID{},
				replication: []ReplID{
					{
						Src: StorageBucketID{
							Storage: "one",
							BucketID: BucketID{
								Account: "a1",
								Bucket:  "b1",
							},
						},
						Dest: StorageBucketID{
							Storage: "one",
							BucketID: BucketID{
								Account: "a1",
								Bucket:  "b2",
							},
						},
					},
				},
			},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b2",
						},
					},
					Dest: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b3",
						},
					},
				},
			},
			wantErr: dom.ErrRoutingBlocked,
		},
		{
			name: "cannot replicate if dest is already used as src on acc level",
			existing: existing{
				routing: []StorageBucketID{},
				replication: []ReplID{
					{
						Src: StorageBucketID{
							Storage: "one",
							BucketID: BucketID{
								Account: "a1",
								Bucket:  "",
							},
						},
						Dest: StorageBucketID{
							Storage: "one",
							BucketID: BucketID{
								Account: "a2",
								Bucket:  "",
							},
						},
					},
				},
			},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a3",
							Bucket:  "b1",
						},
					},
					Dest: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b1",
						},
					},
				},
			},
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "cannot replicate acc if dest is already used as src on acc level",
			existing: existing{
				routing: []StorageBucketID{},
				replication: []ReplID{
					{
						Src: StorageBucketID{
							Storage: "one",
							BucketID: BucketID{
								Account: "a1",
								Bucket:  "",
							},
						},
						Dest: StorageBucketID{
							Storage: "one",
							BucketID: BucketID{
								Account: "a2",
								Bucket:  "",
							},
						},
					},
				},
			},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a3",
							Bucket:  "",
						},
					},
					Dest: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "",
						},
					},
				},
			},
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "cannot replicate if dest is already used as dest",
			existing: existing{
				routing: []StorageBucketID{},
				replication: []ReplID{
					{
						Src: StorageBucketID{
							Storage: "one",
							BucketID: BucketID{
								Account: "a1",
								Bucket:  "b1",
							},
						},
						Dest: StorageBucketID{
							Storage: "two",
							BucketID: BucketID{
								Account: "a1",
								Bucket:  "b1",
							},
						},
					},
				},
			},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a2",
							Bucket:  "b3",
						},
					},
					Dest: StorageBucketID{
						Storage: "two",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b1",
						},
					},
				},
			},
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "cannot replicate if dest is already used as dest on acc lvl",
			existing: existing{
				routing: []StorageBucketID{},
				replication: []ReplID{
					{
						Src: StorageBucketID{
							Storage: "one",
							BucketID: BucketID{
								Account: "a1",
								Bucket:  "",
							},
						},
						Dest: StorageBucketID{
							Storage: "two",
							BucketID: BucketID{
								Account: "a1",
								Bucket:  "",
							},
						},
					},
				},
			},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a2",
							Bucket:  "b3",
						},
					},
					Dest: StorageBucketID{
						Storage: "two",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "b1",
						},
					},
				},
			},
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "cannot replicate to same storage if it blocks existing bucket repl",
			existing: existing{
				routing: []StorageBucketID{},
				replication: []ReplID{
					{
						Src: StorageBucketID{
							Storage: "one",
							BucketID: BucketID{
								Account: "a1",
								Bucket:  "b1",
							},
						},
						Dest: StorageBucketID{
							Storage: "two",
							BucketID: BucketID{
								Account: "a1",
								Bucket:  "b1",
							},
						},
					},
				},
			},
			args: args{
				id: ReplID{
					Src: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a2",
							Bucket:  "",
						},
					},
					Dest: StorageBucketID{
						Storage: "one",
						BucketID: BucketID{
							Account: "a1",
							Bucket:  "",
						},
					},
				},
			},
			wantErr: dom.ErrInvalidArg,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := miniredis.RunT(t)
			c := redis.NewClient(&redis.Options{Addr: db.Addr()})
			ctx := context.TODO()
			r := require.New(t)
			s := &policySvc2{
				client:      c,
				storages:    storages,
				mainStorage: "one",
			}
			for _, route := range tt.existing.routing {
				r.NoError(s.AddRoutingPolicy(ctx, route.BucketID, route.Storage), "add existing routing", route.String())
			}
			for _, repl := range tt.existing.replication {
				r.NoError(s.AddReplicationPolicy(ctx, repl, tasks.Priority3), "add existing routing", repl.String())
			}
			if err := s.AddReplicationPolicy(ctx, tt.args.id, tasks.Priority2); !errors.Is(err, tt.wantErr) {
				t.Errorf("policySvc2.AddReplicationPolicy() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_ReplicationPolicy_e2e(t *testing.T) {
	r := require.New(t)
	db := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: db.Addr()})
	main, second, third, fourth := "main", "second", "third", "fourth"
	b1, b2, b3, a1, a2, a3 := "b1", "b2", "b3", "a1", "a2", "a3"
	storages := map[string]bool{
		main:   true,
		second: false,
		third:  false,
		fourth: false,
	}
	ctx := context.TODO()

	s := NewSvc2(storages, c)

	// add different routing policies
	r.NoError(s.AddRoutingPolicy(ctx, BucketID{
		Account: a2,
		Bucket:  "",
	}, second), "route to second for a2")

	r.NoError(s.AddRoutingPolicy(ctx, BucketID{
		Account: a2,
		Bucket:  b1,
	}, third), "route to third for a2-b1")

	r.NoError(s.AddRoutingPolicy(ctx, BucketID{
		Account: a3,
		Bucket:  b1,
	}, fourth), "route to third for a3-b1")

	// check that routing policies are resolved correctly:
	route, err := s.GetRoutingPolicy(ctx, BucketID{
		Account: a1,
		Bucket:  "",
	})
	r.NoError(err, "default routing to main")
	r.EqualValues(main, route, "default routing to main")

	route, err = s.GetRoutingPolicy(ctx, BucketID{
		Account: a1,
		Bucket:  b1,
	})
	r.NoError(err, "default routing to main")
	r.EqualValues(main, route, "default routing to main")

	route, err = s.GetRoutingPolicy(ctx, BucketID{
		Account: a2,
		Bucket:  "",
	})
	r.NoError(err, "default a2 routing to second")
	r.EqualValues(second, route, "default a2 routing to second")

	route, err = s.GetRoutingPolicy(ctx, BucketID{
		Account: a2,
		Bucket:  b2,
	})
	r.NoError(err, "default a2 routing to second")
	r.EqualValues(second, route, "default a2 routing to second")

	route, err = s.GetRoutingPolicy(ctx, BucketID{
		Account: a2,
		Bucket:  b1,
	})
	r.NoError(err, "a2-b1 routing to third")
	r.EqualValues(third, route, "a2-b1 routing to third")

	route, err = s.GetRoutingPolicy(ctx, BucketID{
		Account: a3,
		Bucket:  b1,
	})
	r.NoError(err, "a3-b1 routing to fourth")
	r.EqualValues(fourth, route, "a2-b1 routing to fourth")

	route, err = s.GetRoutingPolicy(ctx, BucketID{
		Account: a3,
		Bucket:  b2,
	})
	r.NoError(err, "a3-b2 fallbacks to main")
	r.EqualValues(main, route, "a3-b2 fallbacks to main")

	// check violation of routing policies
	r.ErrorContains(s.AddReplicationPolicy(ctx, ReplID{
		Src: StorageBucketID{
			Storage: second,
			BucketID: BucketID{
				Account: a3,
				Bucket:  b2,
			},
		},
		Dest: StorageBucketID{
			Storage: main,
			BucketID: BucketID{
				Account: a1,
				Bucket:  b1,
			},
		},
	}, tasks.Priority2), "different from routing", "routing not from main")

	r.ErrorContains(s.AddReplicationPolicy(ctx, ReplID{
		Src: StorageBucketID{
			Storage: third,
			BucketID: BucketID{
				Account: a2,
				Bucket:  b2,
			},
		},
		Dest: StorageBucketID{
			Storage: main,
			BucketID: BucketID{
				Account: a1,
				Bucket:  b1,
			},
		},
	}, tasks.Priority2), "different from routing", "routing not from second")

	r.ErrorContains(s.AddReplicationPolicy(ctx, ReplID{
		Src: StorageBucketID{
			Storage: second,
			BucketID: BucketID{
				Account: a2,
				Bucket:  b1,
			},
		},
		Dest: StorageBucketID{
			Storage: main,
			BucketID: BucketID{
				Account: a1,
				Bucket:  b1,
			},
		},
	}, tasks.Priority2), "different from routing", "routing not from third")

	// create replications involving routing policies created earlier
	r.NoError(s.AddReplicationPolicy(ctx, ReplID{
		Src: StorageBucketID{
			Storage: main,
			BucketID: BucketID{
				Account: a3,
				Bucket:  b2,
			},
		},
		Dest: StorageBucketID{
			Storage: second,
			BucketID: BucketID{
				Account: a3,
				Bucket:  b3,
			},
		},
	}, tasks.PriorityDefault1), "create bucket replication from main")

	r.NoError(s.AddReplicationPolicy(ctx, ReplID{
		Src: StorageBucketID{
			Storage: second,
			BucketID: BucketID{
				Account: a2,
				Bucket:  b2,
			},
		},
		Dest: StorageBucketID{
			Storage: second,
			BucketID: BucketID{
				Account: a1,
				Bucket:  b3,
			},
		},
	}, tasks.Priority2), "create bucket replication from second")

	r.NoError(s.AddReplicationPolicy(ctx, ReplID{
		Src: StorageBucketID{
			Storage: third,
			BucketID: BucketID{
				Account: a2,
				Bucket:  b1,
			},
		},
		Dest: StorageBucketID{
			Storage: fourth,
			BucketID: BucketID{
				Account: a1,
				Bucket:  b3,
			},
		},
	}, tasks.Priority3), "create bucket replication from third")

	// cannot the same create again
	r.Error(s.AddReplicationPolicy(ctx, ReplID{
		Src: StorageBucketID{
			Storage: main,
			BucketID: BucketID{
				Account: a3,
				Bucket:  b2,
			},
		},
		Dest: StorageBucketID{
			Storage: second,
			BucketID: BucketID{
				Account: a3,
				Bucket:  b3,
			},
		},
	}, tasks.PriorityDefault1), "already exists")

	r.Error(s.AddReplicationPolicy(ctx, ReplID{
		Src: StorageBucketID{
			Storage: second,
			BucketID: BucketID{
				Account: a2,
				Bucket:  b2,
			},
		},
		Dest: StorageBucketID{
			Storage: second,
			BucketID: BucketID{
				Account: a1,
				Bucket:  b3,
			},
		},
	}, tasks.Priority2), "already exists")

	r.Error(s.AddReplicationPolicy(ctx, ReplID{
		Src: StorageBucketID{
			Storage: third,
			BucketID: BucketID{
				Account: a2,
				Bucket:  b1,
			},
		},
		Dest: StorageBucketID{
			Storage: fourth,
			BucketID: BucketID{
				Account: a1,
				Bucket:  b3,
			},
		},
	}, tasks.Priority3), "already exists")

	// add acc lvl replications

	r.ErrorContains(s.AddReplicationPolicy(ctx, ReplID{
		Src: StorageBucketID{
			Storage: second,
			BucketID: BucketID{
				Account: a1,
				Bucket:  "",
			},
		},
		Dest: StorageBucketID{
			Storage: third,
			BucketID: BucketID{
				Account: a1,
				Bucket:  "",
			},
		},
	}, tasks.Priority2), "different from routing", "routing not from main for acc lvl")

	r.NoError(s.AddRoutingPolicy(ctx, BucketID{
		Account: a1,
		Bucket:  "",
	}, second), "roture to second for a1")

	route, err = s.GetRoutingPolicy(ctx, BucketID{
		Account: a1,
		Bucket:  "",
	})
	r.NoError(err)

	r.NoError(s.AddReplicationPolicy(ctx, ReplID{
		Src: StorageBucketID{
			Storage: second,
			BucketID: BucketID{
				Account: a1,
				Bucket:  "",
			},
		},
		Dest: StorageBucketID{
			Storage: third,
			BucketID: BucketID{
				Account: a1,
				Bucket:  "",
			},
		},
	}, tasks.Priority2), "added acc lvl routing a1 to second")

	r.Error(s.AddReplicationPolicy(ctx, ReplID{
		Src: StorageBucketID{
			Storage: second,
			BucketID: BucketID{
				Account: a1,
				Bucket:  "",
			},
		},
		Dest: StorageBucketID{
			Storage: third,
			BucketID: BucketID{
				Account: a1,
				Bucket:  "",
			},
		},
	}, tasks.Priority2), "already exists")
}
