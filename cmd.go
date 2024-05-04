package kvstore

import "strings"

type Cmd struct {
	Name     string
	Args     []string
	FullName string
}

func NewCmd(c string) *Cmd {
	sp := strings.Split(c, " ")
	cmd := &Cmd{FullName: c, Name: sp[0]}
	if len(sp) > 1 {
		cmd.Args = sp[1:]
	}
	return cmd
}
