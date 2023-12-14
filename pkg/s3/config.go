package s3

import (
	"fmt"
	"github.com/clyso/chorus/pkg/dom"
	"sort"
	"time"
)

const (
	defaultHealthCheckInterval = time.Second * 5
	defaultHttpTimeout         = time.Minute * 5
)

type StorageConfig struct {
	Storages          map[string]Storage `yaml:"storages"`
	CreateRouting     bool               `yaml:"createRouting"`
	CreateReplication bool               `yaml:"createReplication"`

	storageList []string
}

type Storage struct {
	Address             string                   `yaml:"address"`
	Credentials         map[string]CredentialsV4 `yaml:"credentials"`
	Provider            string                   `yaml:"provider"`
	IsMain              bool                     `yaml:"isMain"`
	HealthCheckInterval time.Duration            `yaml:"healthCheckInterval"`
	HttpTimeout         time.Duration            `yaml:"httpTimeout"`
	IsSecure            bool                     `yaml:"isSecure"`

	RateLimit RateLimit `yaml:"rateLimit"`

	credentialList []string
}

type RateLimit struct {
	Enabled bool `yaml:"enabled"`
	RPM     int  `yaml:"rpm"`
}

type CredentialsV4 struct {
	AccessKeyID     string `yaml:"accessKeyID"`
	SecretAccessKey string `yaml:"secretAccessKey"`
}

func (s *StorageConfig) RateLimitConf() map[string]RateLimit {
	res := make(map[string]RateLimit, len(s.Storages))
	for name, conf := range s.Storages {
		res[name] = conf.RateLimit
	}
	return res
}

func (s *StorageConfig) StorageList() []string {
	return s.storageList
}

func (s *StorageConfig) Main() string {
	if len(s.storageList) == 0 {
		return ""
	}
	return s.storageList[0]
}

func (s *StorageConfig) Followers() []string {
	if len(s.storageList) == 0 {
		return nil
	}
	return s.storageList[1:]
}

func (s *Storage) CredentialList() []string {
	return s.credentialList
}

func (s *StorageConfig) Init() error {
	if len(s.Storages) == 0 {
		return fmt.Errorf("app config: empty storages config")
	}
	hasMain := false
	users := map[string]struct{}{}
	storList := make([]string, 0, len(s.Storages))
	for name, storage := range s.Storages {
		if len(storage.Credentials) == 0 {
			return fmt.Errorf("%w: app config: storage %q credentials not set", dom.ErrInvalidStorageConfig, name)
		}
		storUsers := map[string]struct{}{}
		storUserList := make([]string, 0, len(storage.Credentials))
		for user, cred := range storage.Credentials {
			if cred.SecretAccessKey == "" {
				return fmt.Errorf("%w: app config: storage %q, user %q: secretAccessKey required", dom.ErrInvalidStorageConfig, name, user)
			}
			if cred.AccessKeyID == "" {
				return fmt.Errorf("%w: app config: storage %q, user %q: accessKeyID required", dom.ErrInvalidStorageConfig, name, user)
			}

			storUsers[user] = struct{}{}
			storUserList = append(storUserList, user)
		}
		sort.Strings(storUserList)
		storage.credentialList = storUserList

		if len(users) == 0 {
			users = storUsers
		}
		if len(users) != len(storUsers) {
			return fmt.Errorf("%w: app config: all storage credentials should contain the same users", dom.ErrInvalidStorageConfig)
		}
		for u := range users {
			if _, ok := storUsers[u]; !ok {
				return fmt.Errorf("%w: app config: storage %q missing credential user %q", dom.ErrInvalidStorageConfig, name, u)
			}
		}

		if storage.IsMain && hasMain {
			return fmt.Errorf("%w: app config: multiple main storages not allowed", dom.ErrInvalidStorageConfig)
		}
		if storage.IsMain {
			hasMain = true
		}

		if storage.HealthCheckInterval == 0 {
			storage.HealthCheckInterval = defaultHealthCheckInterval
		}
		if storage.HttpTimeout == 0 {
			storage.HttpTimeout = defaultHttpTimeout
		}
		if storage.Provider == "" {
			return fmt.Errorf("app config: storage provider required")
		}
		if storage.Address == "" {
			return fmt.Errorf("app config: storage address required")
		}
		s.Storages[name] = storage
		storList = append(storList, name)
	}
	if !hasMain {
		return fmt.Errorf("%w: app config: main storage is not set", dom.ErrInvalidStorageConfig)
	}
	sort.Slice(storList, func(i, j int) bool {
		if s.Storages[storList[i]].IsMain {
			return true
		}
		if s.Storages[storList[j]].IsMain {
			return false
		}
		return storList[i] < storList[j]
	})
	s.storageList = storList

	return nil
}
