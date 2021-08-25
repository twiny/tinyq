# Queue
an implementation of a persistent FIFO queue. with ability to pause/start dequeue.

**NOTE**: This package is provided "as is" with no guarantee. Use it at your own risk and always test it yourself before using it in a production environment. If you find any issues, please [create a new issue](https://github.com/twiny/tinyq/issues/new).

## Install:
`go get -u github.com/twiny/tinyq/...`

## Examples:
see usage [examples](_examples/http/main.go)

## Known Issues:
- [ ] all goroutines are asleep - deadlock caused by not closing `queue.Dequeue` channel. 