package src

import "errors"

var (
	ErrHashNotExist = errors.New("set not exist")

	ErrHashMemberNotExist = errors.New("set member not exist")
)

// Hash 基本上和set一样
type Hash struct {
	M map[string]map[string]*string
}

func NewHash() *Hash {
	h := &Hash{}
	h.M = make(map[string]map[string]*string)
	return h
}

func (s *Hash) HmSet(key string, keys []string, records []*string) error {
	hash, ok := s.M[key]
	if !ok {
		s.M[key] = make(map[string]*string)
		hash = s.M[key]
	}
	for i, value := range keys {
		hash[value] = records[i]
	}
	return nil
}

func (s *Hash) HDel(key string, keys ...string) error {
	hash, ok := s.M[key]
	if !ok {
		return ErrHashNotExist
	}

	if len(keys) == 0 || keys[0] == "" {
		return ErrMemberEmpty
	}

	for _, value := range keys {
		delete(hash, value)
	}

	return nil
}

func (s *Hash) HExists2(key, key2 string) bool {
	if v, ok := s.M[key]; ok {
		if _, ok := v[key2]; ok {
			return true
		}
	}
	return false
}
func (s *Hash) HExists(key string) bool {
	if _, ok := s.M[key]; ok {
		return true
	}
	return false
}

func (s *Hash) HCard(key string) int {
	if !s.HExists(key) {
		return 0
	}

	return len(s.M[key])
}

func (s *Hash) HGetAll(key string) ([]*string, error) {
	if _, ok := s.M[key]; !ok {
		return nil, ErrHashNotExist
	}

	records := make([]*string, 0)

	for _, record := range s.M[key] {
		records = append(records, record)
	}

	return records, nil
}
