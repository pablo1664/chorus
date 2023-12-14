package s3

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStorageConfig_Validate(t *testing.T) {
	s := StorageConfig{
		Storages: map[string]Storage{
			"a": {IsMain: false, Address: "a", Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
			"b": {IsMain: false, Address: "a", Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
			"c": {IsMain: false, Address: "a", Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
			"d": {IsMain: false, Address: "a", Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
			"e": {IsMain: true, Address: "a", Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
			"f": {IsMain: false, Address: "a", Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
			"g": {IsMain: false, Address: "a", Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
		},
	}
	r := require.New(t)
	r.NoError(s.Init())
	res1 := make([]string, len(s.Storages))
	copy(res1, s.storageList)
	r.NoError(s.Init())
	res2 := make([]string, len(s.Storages))
	copy(res2, s.storageList)
	r.NoError(s.Init())
	res3 := make([]string, len(s.Storages))
	copy(res3, s.storageList)
	r.EqualValues(res1, res2)
	r.EqualValues(res3, res2)

	r.EqualValues(res1[0], "e")
	r.EqualValues(res1[1], "a")
	r.EqualValues(res1[2], "b")
	r.EqualValues(res1[3], "c")
	r.EqualValues(res1[4], "d")
	r.EqualValues(res1[5], "f")
	r.EqualValues(res1[6], "g")

	fol := s.Followers()
	r.EqualValues(fol[0], "a")
	r.EqualValues(fol[1], "b")
	r.EqualValues(fol[2], "c")
	r.EqualValues(fol[3], "d")
	r.EqualValues(fol[4], "f")
	r.EqualValues(fol[5], "g")
	r.EqualValues(len(fol), len(res1)-1)
}
