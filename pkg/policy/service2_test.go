package policy

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

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

func Test_replIDfromKey(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want ReplID
	}{
		{
			name: "success",
			key:  "s1:a1:b1:s2:a2:b2",
			want: ReplID{
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
						Bucket:  "b2",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := replIDfromKey(tt.key); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("replIDfromKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_replIDKeyE2E(t *testing.T) {
	tests := []struct {
		name string
		want ReplID
	}{
		{
			name: "success",
			want: ReplID{
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
						Bucket:  "b2",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := tt.want.key()
			if got := replIDfromKey(key); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("replIDfromKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReplID_statusKey(t *testing.T) {
	type fields struct {
		Src  StorageBucketID
		Dest StorageBucketID
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "success",
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
						Bucket:  "b2",
					},
				},
			},
			want: "p:repl_st:s1:a1:b1:s2:a2:b2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := ReplID{
				Src:  tt.fields.Src,
				Dest: tt.fields.Dest,
			}
			if got := r.statusKey(); got != tt.want {
				t.Errorf("ReplID.statusKey() = %v, want %v", got, tt.want)
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
				r.NoError(s.AddReplicationPolicy(ctx, repl, tasks.Priority3), "add existing routing", repl.key())
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
				Bucket:  b2,
			},
		},
	}, tasks.PriorityHighest5), "create second bucket replication from third")

	// cannot create the same replciations twice
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
	r.EqualValues(second, route)

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

	// lookup replication policies:
	_, err = s.GetBucketReplicationPolicies(ctx, StorageBucketID{
		Storage: second,
		BucketID: BucketID{
			Account: a3,
			Bucket:  b3,
		},
	})
	r.ErrorIs(err, dom.ErrNotFound, "no replication found")
	_, err = s.GetBucketReplicationPolicies(ctx, StorageBucketID{
		Storage: main,
		BucketID: BucketID{
			Account: a3,
			Bucket:  "",
		},
	})
	r.ErrorIs(err, dom.ErrInvalidArg, "bucket name required")

	repls, err := s.GetBucketReplicationPolicies(ctx, StorageBucketID{
		Storage: main,
		BucketID: BucketID{
			Account: a3,
			Bucket:  b2,
		},
	})
	r.NoError(err)
	r.Len(repls, 1)
	r.EqualValues(tasks.PriorityDefault1, repls[0].Priority)
	r.EqualValues(second, repls[0].ID.Storage)
	r.EqualValues(a3, repls[0].ID.Account)
	r.EqualValues(b3, repls[0].ID.Bucket)

	repls, err = s.GetBucketReplicationPolicies(ctx, StorageBucketID{
		Storage: second,
		BucketID: BucketID{
			Account: a2,
			Bucket:  b2,
		},
	})
	r.NoError(err)
	r.Len(repls, 1)
	r.EqualValues(tasks.Priority2, repls[0].Priority)
	r.EqualValues(second, repls[0].ID.Storage)
	r.EqualValues(a1, repls[0].ID.Account)
	r.EqualValues(b3, repls[0].ID.Bucket)

	repls, err = s.GetBucketReplicationPolicies(ctx, StorageBucketID{
		Storage: third,
		BucketID: BucketID{
			Account: a2,
			Bucket:  b1,
		},
	})
	r.NoError(err)
	r.Len(repls, 2)
	r.EqualValues(tasks.PriorityHighest5, repls[0].Priority)
	r.EqualValues(fourth, repls[0].ID.Storage)
	r.EqualValues(a1, repls[0].ID.Account)
	r.EqualValues(b2, repls[0].ID.Bucket)

	r.EqualValues(tasks.Priority3, repls[1].Priority)
	r.EqualValues(fourth, repls[1].ID.Storage)
	r.EqualValues(a1, repls[1].ID.Account)
	r.EqualValues(b3, repls[1].ID.Bucket)

	// lookup account level replication
	repls, err = s.GetBucketReplicationPolicies(ctx, StorageBucketID{
		Storage: second,
		BucketID: BucketID{
			Account: a1,
			Bucket:  b1,
		},
	})
	r.NoError(err)
	r.Len(repls, 1)
	r.EqualValues(tasks.Priority2, repls[0].Priority)
	r.EqualValues(third, repls[0].ID.Storage)
	r.EqualValues(a1, repls[0].ID.Account)
	r.EqualValues(b1, repls[0].ID.Bucket)

	repls, err = s.GetBucketReplicationPolicies(ctx, StorageBucketID{
		Storage: second,
		BucketID: BucketID{
			Account: a1,
			Bucket:  b2,
		},
	})
	r.NoError(err)
	r.Len(repls, 1)
	r.EqualValues(tasks.Priority2, repls[0].Priority)
	r.EqualValues(third, repls[0].ID.Storage)
	r.EqualValues(a1, repls[0].ID.Account)
	r.EqualValues(b2, repls[0].ID.Bucket)

	repls, err = s.GetBucketReplicationPolicies(ctx, StorageBucketID{
		Storage: second,
		BucketID: BucketID{
			Account: a1,
			Bucket:  b3,
		},
	})
	r.NoError(err)
	r.Len(repls, 1)
	r.EqualValues(tasks.Priority2, repls[0].Priority)
	r.EqualValues(third, repls[0].ID.Storage)
	r.EqualValues(a1, repls[0].ID.Account)
	r.EqualValues(b3, repls[0].ID.Bucket)

	repls, err = s.GetBucketReplicationPolicies(ctx, StorageBucketID{
		Storage: second,
		BucketID: BucketID{
			Account: a1,
			Bucket:  "asdf",
		},
	})
	r.NoError(err)
	r.Len(repls, 1)
	r.EqualValues(tasks.Priority2, repls[0].Priority)
	r.EqualValues(third, repls[0].ID.Storage)
	r.EqualValues(a1, repls[0].ID.Account)
	r.EqualValues("asdf", repls[0].ID.Bucket)

	// check that routing to followers is blocked
	_, err = s.GetRoutingPolicy(ctx, BucketID{
		Account: a1,
		Bucket:  b3,
	})
	r.ErrorIs(err, dom.ErrRoutingBlocked)

	// delete created policies
	r.ErrorIs(s.DeleteReplicationPolicy(ctx, ReplID{
		Src: StorageBucketID{
			Storage: main,
			BucketID: BucketID{
				Account: a3,
				Bucket:  "qwerty",
			},
		},
		Dest: StorageBucketID{
			Storage: second,
			BucketID: BucketID{
				Account: a3,
				Bucket:  b3,
			},
		},
	}), dom.ErrNotFound)

	r.ErrorIs(s.DeleteReplicationPolicy(ctx, ReplID{
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
				Bucket:  "qwerty",
			},
		},
	}), dom.ErrNotFound)

	r.NoError(s.DeleteReplicationPolicy(ctx, ReplID{
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
	}), "delete replication from main")

	r.NoError(s.DeleteReplicationPolicy(ctx, ReplID{
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
	}), "delete replication from second")
	r.NoError(s.DeleteReplicationPolicy(ctx, ReplID{
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
	}), "delete replication from third")
	r.NoError(s.DeleteReplicationPolicy(ctx, ReplID{
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
				Bucket:  b2,
			},
		},
	}), "delete second replication from third")
	r.NoError(s.DeleteReplicationPolicy(ctx, ReplID{
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
	}), "delete acc replication")

	// check that replciations were deleted
	_, err = s.GetBucketReplicationPolicies(ctx, StorageBucketID{
		Storage: main,
		BucketID: BucketID{
			Account: a3,
			Bucket:  b2,
		},
	})
	r.ErrorIs(err, dom.ErrNotFound)

	_, err = s.GetBucketReplicationPolicies(ctx, StorageBucketID{
		Storage: second,
		BucketID: BucketID{
			Account: a2,
			Bucket:  b2,
		},
	})
	r.ErrorIs(err, dom.ErrNotFound)

	_, err = s.GetBucketReplicationPolicies(ctx, StorageBucketID{
		Storage: third,
		BucketID: BucketID{
			Account: a2,
			Bucket:  b1,
		},
	})

	r.ErrorIs(err, dom.ErrNotFound)
	_, err = s.GetBucketReplicationPolicies(ctx, StorageBucketID{
		Storage: second,
		BucketID: BucketID{
			Account: a1,
			Bucket:  b1,
		},
	})
	r.ErrorIs(err, dom.ErrNotFound)

	// create once again to check that everything were properly deleted
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
				Bucket:  b2,
			},
		},
	}, tasks.PriorityHighest5), "create second bucket replication from third")

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
}

func Test_ReplicationPolicyStatus_e2e(t *testing.T) {
	r := require.New(t)
	db := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: db.Addr()})
	main, second, third := "main", "second", "third"
	b1, a1, a2 := "b1", "a1", "a2"
	ag1, ag2 := "http://ag1.com", "http://ag2.com"
	storages := map[string]bool{
		main:   true,
		second: false,
		third:  false,
	}
	ctx := context.TODO()

	s := NewSvc2(storages, c)

	id1 := ReplID{
		Src: StorageBucketID{
			Storage: main,
			BucketID: BucketID{
				Account: a1,
				Bucket:  b1,
			},
		},
		Dest: StorageBucketID{
			Storage: second,
			BucketID: BucketID{
				Account: a1,
				Bucket:  b1,
			},
		},
	}

	id2 := ReplID{
		Src: StorageBucketID{
			Storage: main,
			BucketID: BucketID{
				Account: a2,
				Bucket:  b1,
			},
		},
		Dest: StorageBucketID{
			Storage: second,
			BucketID: BucketID{
				Account: a2,
				Bucket:  b1,
			},
		},
	}
	id3 := ReplID{
		Src: StorageBucketID{
			Storage: main,
			BucketID: BucketID{
				Account: a2,
				Bucket:  b1,
			},
		},
		Dest: StorageBucketID{
			Storage: third,
			BucketID: BucketID{
				Account: a2,
				Bucket:  b1,
			},
		},
	}

	// create replication policies
	idAcc := id1
	idAcc.Src.Bucket = ""
	idAcc.Dest.Bucket = ""
	r.NoError(s.AddReplicationPolicy(ctx, idAcc, tasks.Priority2), "added acc lvl routing")
	r.NoError(s.AddReplicationPolicy(ctx, id2, tasks.Priority3), "added bucket lvl routing")
	r.NoError(s.AddReplicationPolicy(ctx, id3, tasks.Priority4), "added bucket lvl routing")

	// check that status not exists
	list, err := s.ListReplicationPolicyInfo(ctx)
	r.NoError(err)
	r.Empty(list, "status not exists")

	_, err = s.GetReplicationInfo(ctx, id1)
	r.ErrorIs(err, dom.ErrNotFound, "status not exisis")

	_, err = s.GetReplicationInfo(ctx, id2)
	r.ErrorIs(err, dom.ErrNotFound, "status not exisis")

	_, err = s.GetReplicationInfo(ctx, id3)
	r.ErrorIs(err, dom.ErrNotFound, "status not exisis")

	// add status validation
	r.ErrorIs(s.AddReplicationInfo(ctx, idAcc, nil), dom.ErrInvalidArg, "bucket required")

	idNoPolicy := id2
	idNoPolicy.Src.Bucket = "asdf"
	idNoPolicy.Dest.Bucket = "asdf"
	r.ErrorIs(s.AddReplicationInfo(ctx, idNoPolicy, nil), dom.ErrInvalidArg, "repl policy should be presented")

	r.ErrorIs(s.PauseReplication(ctx, id2), dom.ErrNotFound)
	r.ErrorIs(s.ResumeReplication(ctx, id2), dom.ErrNotFound)
	r.ErrorIs(s.ObjListStarted(ctx, id2), dom.ErrNotFound)
	r.ErrorIs(s.IncReplEvents(ctx, id2, time.Now()), dom.ErrNotFound)
	r.ErrorIs(s.IncReplEventsDone(ctx, id2, time.Now()), dom.ErrNotFound)
	r.ErrorIs(s.IncReplInitObjListed(ctx, id2, 5, time.Now()), dom.ErrNotFound)
	r.ErrorIs(s.IncReplInitObjDone(ctx, id2, 5, time.Now()), dom.ErrNotFound)

	r.NoError(s.AddReplicationInfo(ctx, id1, &ag1))
	r.ErrorIs(s.AddReplicationInfo(ctx, id1, nil), dom.ErrAlreadyExists)
	list, err = s.ListReplicationPolicyInfo(ctx)
	r.NoError(err)
	r.Len(list, 1)

	r.NoError(s.AddReplicationInfo(ctx, id2, &ag2))
	r.ErrorIs(s.AddReplicationInfo(ctx, id2, nil), dom.ErrAlreadyExists)
	list, err = s.ListReplicationPolicyInfo(ctx)
	r.NoError(err)
	r.Len(list, 2)

	r.NoError(s.AddReplicationInfo(ctx, id3, nil))
	r.ErrorIs(s.AddReplicationInfo(ctx, id3, nil), dom.ErrAlreadyExists)
	list, err = s.ListReplicationPolicyInfo(ctx)
	r.NoError(err)
	r.Len(list, 3)

	r.EqualValues(id1, list[0].ReplID)
	r.EqualValues(id2, list[1].ReplID)
	r.EqualValues(id3, list[2].ReplID)

	ri, err := s.GetReplicationInfo(ctx, id1)
	r.NoError(err)
	r.False(ri.CreatedAt.IsZero())
	r.False(ri.IsPaused)
	r.False(ri.ListingStarted)
	r.Zero(ri.Events)
	r.Zero(ri.EventsDone)
	r.Zero(ri.InitObjListed)
	r.Zero(ri.InitObjDone)
	r.Zero(ri.InitBytesListed)
	r.Zero(ri.InitBytesDone)
	r.EqualValues(ag1, ri.AgentURL)

	r.NoError(s.PauseReplication(ctx, id1))
	ri, err = s.GetReplicationInfo(ctx, id1)
	r.NoError(err)
	r.False(ri.CreatedAt.IsZero())
	r.True(ri.IsPaused)
	r.False(ri.ListingStarted)
	r.Zero(ri.Events)
	r.Zero(ri.EventsDone)
	r.Zero(ri.InitObjListed)
	r.Zero(ri.InitObjDone)
	r.Zero(ri.InitBytesListed)
	r.Zero(ri.InitBytesDone)
	r.EqualValues(ag1, ri.AgentURL)

	r.NoError(s.ResumeReplication(ctx, id1))
	ri, err = s.GetReplicationInfo(ctx, id1)
	r.NoError(err)
	r.False(ri.CreatedAt.IsZero())
	r.False(ri.IsPaused)
	r.False(ri.ListingStarted)
	r.Zero(ri.Events)
	r.Zero(ri.EventsDone)
	r.Zero(ri.InitObjListed)
	r.Zero(ri.InitObjDone)
	r.Zero(ri.InitBytesListed)
	r.Zero(ri.InitBytesDone)
	r.EqualValues(ag1, ri.AgentURL)

	r.NoError(s.ObjListStarted(ctx, id1))
	ri, err = s.GetReplicationInfo(ctx, id1)
	r.NoError(err)
	r.False(ri.CreatedAt.IsZero())
	r.False(ri.IsPaused)
	r.True(ri.ListingStarted)
	r.Zero(ri.Events)
	r.Zero(ri.EventsDone)
	r.Zero(ri.InitObjListed)
	r.Zero(ri.InitObjDone)
	r.Zero(ri.InitBytesListed)
	r.Zero(ri.InitBytesDone)
	r.EqualValues(ag1, ri.AgentURL)

	r.NoError(s.IncReplEvents(ctx, id1, time.Now()))
	ri, err = s.GetReplicationInfo(ctx, id1)
	r.NoError(err)
	r.False(ri.CreatedAt.IsZero())
	r.False(ri.IsPaused)
	r.True(ri.ListingStarted)
	r.EqualValues(1, ri.Events)
	r.Zero(ri.EventsDone)
	r.Zero(ri.InitObjListed)
	r.Zero(ri.InitObjDone)
	r.Zero(ri.InitBytesListed)
	r.Zero(ri.InitBytesDone)
	r.EqualValues(ag1, ri.AgentURL)

	r.NoError(s.IncReplEventsDone(ctx, id1, time.Now()))
	ri, err = s.GetReplicationInfo(ctx, id1)
	r.NoError(err)
	r.False(ri.CreatedAt.IsZero())
	r.False(ri.IsPaused)
	r.True(ri.ListingStarted)
	r.EqualValues(1, ri.Events)
	r.EqualValues(1, ri.EventsDone)
	r.Zero(ri.InitObjListed)
	r.Zero(ri.InitObjDone)
	r.Zero(ri.InitBytesListed)
	r.Zero(ri.InitBytesDone)
	r.EqualValues(ag1, ri.AgentURL)

	r.NoError(s.IncReplInitObjListed(ctx, id1, 5, time.Now()))
	ri, err = s.GetReplicationInfo(ctx, id1)
	r.NoError(err)
	r.False(ri.CreatedAt.IsZero())
	r.False(ri.IsPaused)
	r.True(ri.ListingStarted)
	r.EqualValues(1, ri.Events)
	r.EqualValues(1, ri.EventsDone)
	r.EqualValues(1, ri.InitObjListed)
	r.Zero(ri.InitObjDone)
	r.EqualValues(5, ri.InitBytesListed)
	r.Zero(ri.InitBytesDone)
	r.EqualValues(ag1, ri.AgentURL)

	r.NoError(s.IncReplInitObjDone(ctx, id1, 7, time.Now()))
	ri, err = s.GetReplicationInfo(ctx, id1)
	r.NoError(err)
	r.False(ri.CreatedAt.IsZero())
	r.False(ri.IsPaused)
	r.True(ri.ListingStarted)
	r.EqualValues(1, ri.Events)
	r.EqualValues(1, ri.EventsDone)
	r.EqualValues(1, ri.InitObjListed)
	r.EqualValues(1, ri.InitObjDone)
	r.EqualValues(5, ri.InitBytesListed)
	r.EqualValues(7, ri.InitBytesDone)
	r.EqualValues(ag1, ri.AgentURL)

	ri, err = s.GetReplicationInfo(ctx, id2)
	r.NoError(err)
	r.False(ri.CreatedAt.IsZero())
	r.False(ri.IsPaused)
	r.False(ri.ListingStarted)
	r.Zero(ri.Events)
	r.Zero(ri.EventsDone)
	r.Zero(ri.InitObjListed)
	r.Zero(ri.InitObjDone)
	r.Zero(ri.InitBytesListed)
	r.Zero(ri.InitBytesDone)
	r.EqualValues(ag2, ri.AgentURL)

	ri, err = s.GetReplicationInfo(ctx, id3)
	r.NoError(err)
	r.False(ri.CreatedAt.IsZero())
	r.False(ri.IsPaused)
	r.False(ri.ListingStarted)
	r.Zero(ri.Events)
	r.Zero(ri.EventsDone)
	r.Zero(ri.InitObjListed)
	r.Zero(ri.InitObjDone)
	r.Zero(ri.InitBytesListed)
	r.Zero(ri.InitBytesDone)
	r.Empty(ri.AgentURL)

	// delete statuses
	r.ErrorIs(s.DeleteReplicationInfo(ctx, idNoPolicy), dom.ErrNotFound)

	r.NoError(s.DeleteReplicationInfo(ctx, id1))
	list, err = s.ListReplicationPolicyInfo(ctx)
	r.NoError(err)
	r.Len(list, 2)
	r.ErrorIs(s.DeleteReplicationInfo(ctx, id1), dom.ErrNotFound)

	r.NoError(s.DeleteReplicationInfo(ctx, id2))
	list, err = s.ListReplicationPolicyInfo(ctx)
	r.NoError(err)
	r.Len(list, 1)

	r.NoError(s.DeleteReplicationInfo(ctx, id3))
	list, err = s.ListReplicationPolicyInfo(ctx)
	r.NoError(err)
	r.Empty(list)

	// create again
	ag21, ag22, ag23 := "ag21", "ag22", "ag23"
	r.NoError(s.AddReplicationInfo(ctx, id1, &ag21))
	ri, err = s.GetReplicationInfo(ctx, id1)
	r.NoError(err)
	r.EqualValues(ag21, ri.AgentURL)
	r.NoError(s.AddReplicationInfo(ctx, id2, &ag22))
	ri, err = s.GetReplicationInfo(ctx, id2)
	r.NoError(err)
	r.EqualValues(ag22, ri.AgentURL)
	r.NoError(s.AddReplicationInfo(ctx, id3, &ag23))
	ri, err = s.GetReplicationInfo(ctx, id3)
	r.NoError(err)
	r.EqualValues(ag23, ri.AgentURL)

	list, err = s.ListReplicationPolicyInfo(ctx)
	r.NoError(err)
	r.Len(list, 3)
	r.EqualValues(ag21, list[0].AgentURL)
	r.EqualValues(ag22, list[1].AgentURL)
	r.EqualValues(ag23, list[2].AgentURL)
}

func Test_policySvc2_hSetKeyExists(t *testing.T) {
	r := require.New(t)
	db := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: db.Addr()})
	main, second := "main", "second"
	storages := map[string]bool{
		main:   true,
		second: false,
	}
	ctx := context.TODO()
	s := NewSvc2(storages, c)

	exists, err := s.client.Exists(ctx, "key").Result()
	r.NoError(err)
	r.EqualValues(0, exists)
	r.ErrorIs(s.hSetKeyExists(ctx, "key", "field", "val"), dom.ErrNotFound, "no update if no redis hash")
	exists, err = s.client.Exists(ctx, "key").Result()
	r.NoError(err)
	r.EqualValues(0, exists)

	r.NoError(s.client.HSet(ctx, "key", "field", "val").Err(), "create hash first")
	r.NoError(s.hSetKeyExists(ctx, "key", "field", "val2"), "no update if redis hash exists")
	val, err := s.client.HGet(ctx, "key", "field").Result()
	r.NoError(err)
	r.EqualValues("val2", val, "value updated")

	exists, err = s.client.Exists(ctx, "key").Result()
	r.NoError(err)
	r.EqualValues(1, exists)
}

func Test_policySvc2_incIfKeyExists(t *testing.T) {
	r := require.New(t)
	db := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: db.Addr()})
	main, second := "main", "second"
	storages := map[string]bool{
		main:   true,
		second: false,
	}
	ctx := context.TODO()
	s := NewSvc2(storages, c)

	exists, err := s.client.Exists(ctx, "key").Result()
	r.NoError(err)
	r.EqualValues(0, exists)
	r.ErrorIs(s.incIfKeyExists(ctx, "key", "field", 7), dom.ErrNotFound, "no update if no redis hash")
	exists, err = s.client.Exists(ctx, "key").Result()
	r.NoError(err)
	r.EqualValues(0, exists)

	r.NoError(s.client.HSet(ctx, "key", "field", 3).Err(), "create hash first")
	r.NoError(s.incIfKeyExists(ctx, "key", "field", 7), "no update if redis hash exists")
	val, err := s.client.HGet(ctx, "key", "field").Result()
	r.NoError(err)
	r.EqualValues("10", val, "value updated")

	exists, err = s.client.Exists(ctx, "key").Result()
	r.NoError(err)
	r.EqualValues(1, exists)
}
