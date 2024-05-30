package policy

import (
	"context"
	"reflect"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/clyso/chorus/pkg/dom"
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
	r.ErrorIs(s.addRoutingBlockPolicy(ctx, BucketID{Account: a1}), dom.ErrInvalidArg, "cannot block account")
	r.ErrorIs(s.addRoutingBlockPolicy(ctx, BucketID{Account: a2}), dom.ErrInvalidArg, "cannot block account")
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
