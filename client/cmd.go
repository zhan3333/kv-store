package client

import (
	"context"
	"errors"
	"fmt"
	"strconv"
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
	_ Cmder = (*IntCmd)(nil)
)

/* status command*/

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

/* string command*/

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

func (s *StringCmd) setArgs(args ...string) {
	s.args = args
}

func (s *StringCmd) appendArgs(args ...string) {
	s.args = append(s.args, args...)
}

/* string slice command*/

type StringSliceCmd struct {
	baseCmd

	vals []string
}

func NewStringSliceCmd(ctx context.Context, args ...string) *StringSliceCmd {
	return &StringSliceCmd{
		baseCmd: baseCmd{ctx: ctx, args: args},
	}
}

func (s *StringSliceCmd) String() string {
	return strings.Join(s.args, " ")
}

func (s *StringSliceCmd) setReplay(resp string) {
	s.vals = strings.Split(resp, ",")
}

func (s *StringSliceCmd) appendArgs(args ...string) {
	s.args = append(s.args, args...)
}

func (s *StringSliceCmd) Result() ([]string, error) {
	return s.vals, s.err
}

/* int command*/

type IntCmd struct {
	baseCmd

	val int
}

func NewIntCmd(ctx context.Context, args ...string) *IntCmd {
	return &IntCmd{
		baseCmd: baseCmd{ctx: ctx, args: args},
	}
}

func (i *IntCmd) String() string {
	return strings.Join(i.args, " ")
}

func (i *IntCmd) setReplay(resp string) {
	v, err := strconv.Atoi(resp)
	if err != nil {
		i.SetErr(fmt.Errorf("parse response %s failed: %w", resp, err))
		return
	}
	i.val = v
}

func (i *IntCmd) Result() (int, error) {
	return i.val, i.err
}

/* commands */

func (c cmdable) Ping(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd(ctx, "ping")
	_ = c(ctx, cmd)

	return cmd
}

func (c cmdable) Set(ctx context.Context, kvs ...string) *StringCmd {
	cmd := NewStringCmd(ctx, "set")

	if len(kvs) == 0 || len(kvs)%2 != 0 {
		cmd.SetErr(errors.New("invalid kvs number"))
	}

	cmd.appendArgs(kvs...)
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

func (c cmdable) Del(ctx context.Context, keys ...string) *StringCmd {
	cmd := NewStringCmd(ctx, "del")
	if len(keys) == 0 {
		cmd.SetErr(errors.New("invalid keys number"))
		return cmd
	}
	cmd.appendArgs(keys...)

	_ = c(ctx, cmd)

	return cmd
}

func (c cmdable) LPush(ctx context.Context, key string, values ...string) *StringCmd {
	cmd := NewStringCmd(ctx, "lpush")

	if key == "" {
		cmd.SetErr(errors.New("invalid key"))
		return cmd
	}
	if len(values) == 0 {
		cmd.SetErr(errors.New("invalid values number"))
		return cmd
	}

	cmd.appendArgs(key)
	cmd.appendArgs(values...)

	_ = c(ctx, cmd)

	return cmd
}

func (c cmdable) RPush(ctx context.Context, key string, values ...string) *StringCmd {
	cmd := NewStringCmd(ctx, "rpush")

	if key == "" {
		cmd.SetErr(errors.New("invalid key"))
		return cmd
	}
	if len(values) == 0 {
		cmd.SetErr(errors.New("invalid values number"))
		return cmd
	}

	cmd.appendArgs(key)
	cmd.appendArgs(values...)

	_ = c(ctx, cmd)

	return cmd
}

func (c cmdable) LPop(ctx context.Context, key string, n int) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "lpop")

	if key == "" {
		cmd.SetErr(errors.New("invalid key"))
		return cmd
	}
	if n < 1 {
		cmd.SetErr(errors.New("invalid n value"))
		return cmd
	}

	cmd.appendArgs(key)
	cmd.appendArgs(strconv.Itoa(n))

	_ = c(ctx, cmd)

	return cmd
}

func (c cmdable) LLen(ctx context.Context, key string) *IntCmd {
	cmd := NewIntCmd(ctx, "llen", key)

	if key == "" {
		cmd.SetErr(errors.New("invalid key"))
		return cmd
	}

	_ = c(ctx, cmd)

	return cmd
}
