package src

func Get(db *SaveDBTables, args []string) Result {
	key := args[0]
	s, ok := db.Data.GetWithLock(key)
	if ok {
		db.AllKeys.ActivateKey(args[0])
		if _, ok := s.(*string); !ok {
			return CreateStrResult(C_ERR, "type conversion error")
		}
		return CreateStrResult(C_OK, *s.(*string))
	}
	return CreateStrResult(C_ERR, "key not exist")
}

func SetExc(db *SaveDBTables, arg []string) Result {
	db.Data.PutWithLock(arg[0], &arg[1])
	db.AllKeys.PutKey(arg[0], TypeStr)
	return CreateResult(C_OK, StringToBytes(OK_STR))
}
