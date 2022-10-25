package varint

import (
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"
)

func TestLen(t *testing.T) {
	tm := time.Now()
	for i := 0; i < 100_000_000_000; i++ {
		n := i // rand.Int()
		n1 := Len(n)
		n2 := LenSlow(n)
		if n1 != n2 {
			log.Panicln("not equal at", i, n, n1, n2)
		}
	}
	fmt.Println(time.Since(tm))

	tm = time.Now()
	for i := 0; i < 1000_000_000; i++ {
		n := rand.Int()
		n1 := Len(n)
		n2 := LenSlow(n)
		if n1 != n2 {
			log.Panicln("not equal at", i, n, n1, n2)
		}
	}
	fmt.Println(time.Since(tm))

	tm = time.Now()
	sum := 0
	for i := 0; i < 100_000_000; i++ {
		sum += Len(i)
	}
	fmt.Println("len", time.Since(tm))

	tm = time.Now()
	sum = 0
	for i := 0; i < 100_000_000; i++ {
		sum += LenSlow(i)
	}
	fmt.Println("slow", time.Since(tm))
}
