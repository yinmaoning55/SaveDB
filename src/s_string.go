package src

func Get(db *saveDBTables, args []string) Result {
	key := args[0]
	s, ok := db.Data.Get(key)
	if ok {
		db.AllKeys.ActivateKey(args[0])
		return CreateStrResult(C_OK, *s.(*string))
	}
	return CreateStrResult(C_ERR, "key not exist")
}

func SetExc(db *saveDBTables, arg []string) Result {
	db.Data.Put(arg[0], &arg[1])
	db.AllKeys.PutKey(arg[0], TypeStr)
	return CreateResult(C_OK, StringToBytes(OK_STR))
}
