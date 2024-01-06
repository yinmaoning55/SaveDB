package src

func StringToBytes(s string) []byte {
	return []byte(s)
}
func BytesToString(b []byte) string {
	return string(b)
}

type Result struct {
	Status byte
	Res    []byte
}

func CreateResult(status byte, res []byte) Result {
	if res == nil {
		res = make([]byte, 0)
	}
	return Result{Status: status, Res: res}
}
func CreateIntResult(status byte, i int64) Result {
	res := make([]byte, 0)
	writeInt64(res, 0, i)
	return Result{Status: status, Res: res}
}
func CreateStrResult(status byte, res string) Result {
	var b []byte
	if res == "" {
		b = make([]byte, 0)
	} else {
		b = []byte(res)
	}
	return Result{Status: status, Res: b}
}

func Equals(a interface{}, b interface{}) bool {
	sliceA, okA := a.([]byte)
	sliceB, okB := b.([]byte)
	if okA && okB {
		return BytesEquals(sliceA, sliceB)
	}
	return a == b
}
func BytesEquals(a []byte, b []byte) bool {
	if (a == nil && b != nil) || (a != nil && b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	size := len(a)
	for i := 0; i < size; i++ {
		av := a[i]
		bv := b[i]
		if av != bv {
			return false
		}
	}
	return true
}
