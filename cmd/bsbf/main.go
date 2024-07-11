package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/wzshiming/bsbf"
)

var (
	k string
	l string
	s int
)

func init() {
	flag.StringVar(&k, "k", " ", "key separator")
	flag.StringVar(&l, "l", "\n", "line separator")
	flag.IntVar(&s, "s", 1<<18, "size of sort line")
	flag.Parse()
}

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) < 2 {
		fmt.Println("usage: bsbs [command] <FILE>")
		fmt.Println("       bsbs sort <FILE>")
		fmt.Println("       bsbs get <FILE> key")
		return
	}

	bs := bsbf.NewBSBF(
		bsbf.WithPath(args[1]),
		bsbf.WithLineSep([]byte(l)),
		bsbf.WithKeySepFunc(bsbf.KeySeparator([]byte(k))),
	)
	switch args[0] {
	case "sort":
		if len(args) != 2 {
			fmt.Println("usage: bsbs sort <FILE>")
			return
		}
		err := bs.Sort(s)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}
	case "get":
		if len(args) != 3 {
			fmt.Println("usage: bsbs get <FILE> <key>")
			return
		}

		if strings.Contains(args[2], k) {
			fmt.Println("      key can't contain key separator")
			return
		}

		iter, ok, err := bs.Search([]byte(args[2]))
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}
		if !ok {
			fmt.Println("not found")
		} else {
			os.Stdout.Write(iter.Value())
		}
	}
}
