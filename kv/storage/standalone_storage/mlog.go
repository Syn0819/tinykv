package standalone_storage

import (
	"fmt"
	"log"
	"os"
)

const Debug = true

func LogPrint(format string, a ...interface{}) (n int, err error) {
	if Debug {
		logFile, err := os.OpenFile("/home/syn0819/syn/tinykv/mylog/raft.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			fmt.Println("open log file failed erro: ", err)
			return 0, err
		}
		log.SetOutput(logFile)
		log.SetFlags(log.Llongfile | log.Lmicroseconds | log.Ldate)
		log.Printf(format, a...)
	}
	return
}
