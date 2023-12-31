package src

import (
	"errors"
)

// Set 表的实现直接使用go的map,在此之前需要了解go中的map基本机制
// 1、go中的map由多个bucket组成，每个bucket分为三个部分，tophash区域、keys区域、values区域，都是由内存连续的数组组成
// 2、扩容机制类似于redis的渐进式rehash，map的LoadFactor是6.5
// 3、如果 key 或 value 的数据长度大于一定数值(128)，那么运行时不会在 bucket 中直接存储数据，而是会存储 key 或 value 数据的指针。
// 4、go中的hashcode是吧key的hashcode一分为二，其中低位区的值用于选定 bucket，高位区的值用于在某个 bucket 中确定 key 的位置
var (
	ErrSetNotExist = errors.New("set not exist")

	ErrSetMemberNotExist = errors.New("set member not exist")

	ErrMemberEmpty = errors.New("item empty")
)

type Set struct {
	M map[string]map[string]*struct{}
}

func NewSet() *Set {
	s := &Set{}
	s.M = make(map[string]map[string]*struct{})
	return s
}

func (s *Set) SAdd(key string, values []string) (int, error) {
	set, ok := s.M[key]
	if !ok {
		s.M[key] = make(map[string]*struct{})
		set = s.M[key]
	}
	for _, value := range values {
		set[value] = &struct{}{}
	}
	return len(values), nil
}

func (s *Set) SMove(key string, values ...string) (int, error) {
	set, ok := s.M[key]
	if !ok {
		return -1, ErrSetNotExist
	}

	if len(values) == 0 || values[0] == "" {
		return -1, ErrMemberEmpty
	}

	for _, value := range values {
		delete(set, value)
	}

	return len(values), nil
}

func (s *Set) SHasKey(key string) bool {
	if _, ok := s.M[key]; ok {
		return true
	}
	return false
}

func (s *Set) SPop(key string) *string {
	if !s.SHasKey(key) {
		return nil
	}

	for v, _ := range s.M[key] {
		delete(s.M[key], v)
		return &v
	}

	return nil
}

func (s *Set) SCard(key string) int {
	if !s.SHasKey(key) {
		return 0
	}

	return len(s.M[key])
}

func (s *Set) SDiff(key1, key2 string) ([]*string, error) {
	if !s.SHasKey(key1) || !s.SHasKey(key2) {
		return nil, ErrSetNotExist
	}

	records := make([]*string, 0)

	for v, _ := range s.M[key1] {
		if _, ok := s.M[key2][v]; !ok {
			records = append(records, &v)
		}
	}
	return records, nil
}

func (s *Set) SInter(key1, key2 string) ([]*string, error) {
	if !s.SHasKey(key1) || !s.SHasKey(key2) {
		return nil, ErrSetNotExist
	}

	records := make([]*string, 0)

	for v, _ := range s.M[key1] {
		if _, ok := s.M[key2][v]; ok {
			records = append(records, &v)
		}
	}
	return records, nil
}

func (s *Set) SIsMember(key string, value string) (bool, error) {
	if _, ok := s.M[key]; !ok {
		return false, ErrSetNotExist
	}

	if _, ok := s.M[key][value]; ok {
		return true, nil
	}

	return false, nil
}

func (s *Set) SAreMembers(key string, values ...string) (bool, error) {
	if _, ok := s.M[key]; !ok {
		return false, ErrSetNotExist
	}

	for _, value := range values {
		if _, ok := s.M[key][value]; !ok {
			return false, nil
		}
	}

	return true, nil
}

func (s *Set) SMembers(key string) ([]*string, error) {
	if _, ok := s.M[key]; !ok {
		return nil, ErrSetNotExist
	}
	records := make([]*string, 0)
	for k, _ := range s.M[key] {
		records = append(records, &k)
	}
	return records, nil
}

func (s *Set) SUnion(key1, key2 string) ([]*string, error) {
	if !s.SHasKey(key1) || !s.SHasKey(key2) {
		return nil, ErrSetNotExist
	}

	records, err := s.SMembers(key1)

	if err != nil {
		return nil, err
	}

	for v, _ := range s.M[key2] {
		if _, ok := s.M[key1][v]; !ok {
			records = append(records, &v)
		}
	}

	return records, nil
}
