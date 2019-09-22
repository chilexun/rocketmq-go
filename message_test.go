package mqclient

import (
	"fmt"
	"testing"
)

func TestGenerateUniqMsgID(t *testing.T) {
	msgID1 := GenerateUniqMsgID()
	msgID2 := GenerateUniqMsgID()
	if msgID1 != msgID2 {
		fmt.Println(msgID1)
		fmt.Println(msgID2)
	} else {
		fmt.Println(msgID1)
		t.FailNow()
	}
}
