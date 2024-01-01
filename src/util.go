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
func CreateStrResult(status byte, res string) Result {
	var b []byte
	if res == "" {
		b = make([]byte, 0)
	} else {
		b = []byte(res)
	}
	return Result{Status: status, Res: b}
}
