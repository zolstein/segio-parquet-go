// Copyright (c) 2023 Clumio All Rights Reserved

package main

import (
	"bytes"
	"fmt"
	"github.com/parquet-go/parquet-go"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
)

type TestStruct struct {
	Field1 int64
	Field2 int64
	Field3 int64
}

var (
	cpu   = true
	mem   = true
	write = true
	read  = false
	files = false
	bloom = false
)

func main() {
	if mem {
		runtime.MemProfileRate = 1
	}
	if cpu {
		f, err := os.Create("cpu.pprof")
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	entries := make([]TestStruct, 1024)

	if write {
		// buf := zolsteinNewBuffer(3000000000)

		/*
			outFile, err := os.Create("test.parquet")
			if err != nil {
				log.Fatalf("failed to open parquet file: %v", err)
			}
		*/

		var writer io.Writer
		// writer = buf
		// writer = outFile
		writer = io.Discard

		opts := []parquet.WriterOption{parquet.WriteBufferSize(0)}
		opts = append(opts, parquet.ColumnPageBuffers(parquet.NewChunkBufferPool(256*1024)))
		if files {
			opts = append(opts, parquet.ColumnPageBuffers(parquet.NewFileBufferPool("/tmp", "buffers.*")))
		}
		if bloom {
			opts = append(opts, parquet.BloomFilters(
				parquet.SplitBlockFilter(10, "Field1"),
				parquet.SplitBlockFilter(10, "Field2"),
				parquet.SplitBlockFilter(10, "Field3"),
			))
		}
		pw := parquet.NewGenericWriter[TestStruct](writer, opts...)

		x := int64(0)
		for i := 0; i < 100000; i++ {
			if i%1000 == 0 {
				log.Printf("Iteration: %d", i)
			}
			for j := range entries {
				entries[j] = TestStruct{Field1: x, Field2: x, Field3: x}
				x++
			}
			n, err := pw.Write(entries)
			if err != nil {
				log.Fatalf("failed to write to parquet file: %v", err)
			}
			if n != len(entries) {
				log.Fatalf("only wrote %d entries, not %d", n, len(entries))
			}
		}
		if err := pw.Close(); err != nil {
			log.Fatalf("failed to close parquet writer: %v", err)
		}

		// log.Printf("TotalWriteCalls: %d, TotalCopyCalls: %d", parquet.TotalWriteCalls, parquet.TotalCopyCalls)

		/*
			outFile, err := os.Create("test.parquet")
			if err != nil {
				log.Fatalf("failed to open parquet file: %v", err)
			}

			if _, err := io.Copy(outFile, bytes.NewReader(buf.Bytes())); err != nil {
				log.Fatalf("failed to write file: %v")
			}

			if err := outFile.Close(); err != nil {
				log.Fatalf("failed to close file: %v", err)
			}
		*/
	}

	if read {
		inFile, err := os.Open("test.parquet")
		if err != nil {
			log.Fatalf("failed to open parquet file: %v", err)
		}
		defer inFile.Close()
		/*
			buf := bytes.Buffer{}
			io.Copy(&buf, inFile)
		*/
		buf := readFile(2500000000, inFile)
		r := bytes.NewReader(buf)
		pr := parquet.NewGenericReader[TestStruct](r)
		defer pr.Close()
		for {
			n, err := pr.Read(entries)
			for i := range entries[:n] {
				fmt.Printf("%v\n", entries[i])
			}

			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatalf("failed to read parquet entries: %v\n", err)
			}
		}
	}

	if mem {
		f, err := os.Create("mem.pprof")
		if err != nil {
			log.Fatalf("failed to open file: %v", err)
		}
		defer f.Close()
		if err := pprof.Lookup("allocs").WriteTo(f, 0); err != nil {
			log.Fatalf("failed to write heap profile: %v", err)
		}
	}
}

func readFile(size int, f *os.File) []byte {
	buf := make([]byte, size)
	numRead := 0
	for {
		n, err := f.Read(buf[numRead:])
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("Error reading file: %v", err)
		}
		numRead += n
	}
	return buf[:numRead]
}

func zolsteinNewBuffer(size int64) *bytes.Buffer {
	return bytes.NewBuffer(make([]byte, 0, size))
}
