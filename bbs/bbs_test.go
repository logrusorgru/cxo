package bbs

import (
	"testing"
	"fmt"
)

func Test_Bbs_1(T *testing.T) {
	bbs:= CreateBbs()
	board, _ := bbs.AddBoard("Test Board")
	fmt.Println("board1", board)
	bbs.AddBoard("Another Board")

	bbs.AddThread(board, "Thread 1")

	boards := bbs.AllBoars()
	fmt.Println("boards", boards)

	if (boards[0].Name != "Test Board"){
		T.Fatal("Board name is not equal")
	}

	threads := bbs.GetThreads(boards[0])

	if (threads[0].Name != "Thread 1"){
		T.Fatal("Thread name is not equal")
	}
}