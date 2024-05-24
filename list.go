package kvstore

type List struct {
	Values []string
}

func (l *List) LPush(values ...string) {
	for _, v := range values {
		l.Values = append([]string{v}, l.Values...)
	}
}
