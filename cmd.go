package kvstore

import (
	"context"
	"strings"
)

type cmdable func(ctx context.Context, cmd Cmder) error

type Cmder interface {
	// String output command
	String() string
	SetErr(err error)
	Err() error
	setReplay(resp string)
}

type baseCmd struct {
	args []string
	ctx  context.Context
	err  error
}

func (c baseCmd) SetErr(err error) {
	c.err = err
}

func (c baseCmd) Err() error {
	return c.err
}

var (
	_ Cmder = (*StatusCmd)(nil)
	_ Cmder = (*StringCmd)(nil)
	_ Cmder = (*StringSliceCmd)(nil)
)

type StatusCmd struct {
	baseCmd

	val string
}

func (s *StatusCmd) setReplay(resp string) {
	s.SetVal(resp)
}

func (s *StatusCmd) String() string {
	return strings.Join(s.args, " ")
}

func (s *StatusCmd) SetVal(val string) {
	s.val = val
}

func (s *StatusCmd) Val() string {
	return s.val
}

func (s *StatusCmd) Result() (string, error) {
	return s.val, s.err
}

func (s *StatusCmd) Err() error {
	return s.err
}

func NewStatusCmd(ctx context.Context, args ...string) *StatusCmd {
	return &StatusCmd{
		baseCmd: baseCmd{ctx: ctx, args: args},
	}
}

type StringCmd struct {
	baseCmd

	val string
}

func NewStringCmd(ctx context.Context, args ...string) *StringCmd {
	return &StringCmd{
		baseCmd: baseCmd{ctx: ctx, args: args},
	}
}

func (s *StringCmd) String() string {
	return strings.Join(s.args, " ")
}

func (s *StringCmd) Result() (string, error) {
	return s.val, s.err
}

func (s *StringCmd) setReplay(resp string) {
	s.val = resp
}

func (c cmdable) Ping(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd(ctx, "ping")
	_ = c(ctx, cmd)

	return cmd
}

func (c cmdable) Set(ctx context.Context, key string, val string) *StringCmd {
	cmd := NewStringCmd(ctx, "set", key, val)
	_ = c(ctx, cmd)

	return cmd
}

func (c cmdable) Get(ctx context.Context, key string) *StringCmd {
	cmd := NewStringCmd(ctx, "get", key)
	_ = c(ctx, cmd)

	return cmd
}

func (c cmdable) Keys(ctx context.Context) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "keys")
	_ = c(ctx, cmd)

	return cmd
}

type StringSliceCmd struct {
	baseCmd

	vals []string
}

func (s *StringSliceCmd) String() string {
	return strings.Join(s.args, " ")
}

func NewStringSliceCmd(ctx context.Context, args ...string) *StringSliceCmd {
	return &StringSliceCmd{
		baseCmd: baseCmd{ctx: ctx, args: args},
	}
}

func (s *StringSliceCmd) setReplay(resp string) {
	s.vals = strings.Split(resp, " ")
}

func (s *StringSliceCmd) Result() ([]string, error) {
	return s.vals, s.err
}
