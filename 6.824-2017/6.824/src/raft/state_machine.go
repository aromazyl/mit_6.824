//
// state_machine.go
// Copyright (C) 2017 zhangyule <zyl2336709@gmail.com>
//
// Distributed under terms of the MIT license.
//

package raft

type StateMachine struct {
}

func (sm *StateMachine) Update(entry *LogEntry) StateMachine {
	return StateMachine{}
}
