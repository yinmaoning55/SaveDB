package src

func Get(db *SaveDBTables, args []string) Result {
	key := args[0]
	s, ok := db.Data.GetWithLock(key)
	if ok {
		db.AllKeys.ActivateKey(args[0])
		if _, ok := s.([]byte); !ok {
			return CreateStrResult(CErr, "type conversion error")
		}
		return CreateStrResult(COk, string(s.([]byte)))
	}
	return CreateStrResult(CErr, "key not exist")
}

func SetExc(db *SaveDBTables, arg []string) Result {
	db.Data.PutWithLock(arg[0], []byte(arg[1]))
	db.AllKeys.PutKey(arg[0], TypeStr)
	db.addAof(ToCmdLine2("set", arg...))
	return CreateResult(COk, StringToBytes(OkStr))
}
