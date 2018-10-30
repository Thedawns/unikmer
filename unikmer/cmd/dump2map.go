// Copyright © 2018 Wei Shen <shenwei356@gmail.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"runtime"

	"github.com/tylertreat/BoomFilters"

	"github.com/shenwei356/unikmer"
	"github.com/spf13/cobra"
)

// dump2map represents
var dump2mapCmd = &cobra.Command{
	Use:   "dump2map",
	Short: "serialize Kmers in map[uint64]struct{}",
	Long: `serialize Kmers in map[uint64]struct{}

`,
	Run: func(cmd *cobra.Command, args []string) {
		opt := getOptions(cmd)
		runtime.GOMAXPROCS(opt.NumCPUs)

		var err error

		var files []string
		infileList := getFlagString(cmd, "infile-list")
		if infileList != "" {
			files, err = getListFromFile(infileList)
			checkError(err)
		} else {
			files = getFileList(args)
		}

		if len(files) > 1 {
			checkError(fmt.Errorf("no more than one file should be given"))
		}

		checkFiles(files)

		var infh *bufio.Reader
		var r *os.File
		var reader *unikmer.Reader
		var kcode unikmer.KmerCode

		file := files[0]

		infh, r, _, err = inStream(file)
		checkError(err)
		defer r.Close()

		reader, err = unikmer.NewReader(infh)
		checkError(err)

		if reader.Flag&unikmer.UNIK_SORTED == 0 {
			checkError(fmt.Errorf("input .unik file should be sorted"))
		}

		ibf := boom.NewInverseBloomFilter(uint(float64(reader.Number) * 1.2))

		// ibf.SetHash(fnv.New64a())
		buf := make([]byte, 8)
		be := binary.BigEndian
		for {
			kcode, err = reader.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				checkError(err)
			}

			be.PutUint64(buf, kcode.Code)
			ibf.Add(buf)
		}

		outFile := getFlagString(cmd, "out-prefix")

		if !isStdout(outFile) {
			outFile += extMapDataFile
		}
		outfh, gw, w, err := outStream(outFile, opt.Compress, opt.CompressionLevel)
		checkError(err)
		defer func() {
			outfh.Flush()
			if gw != nil {
				gw.Close()
			}
			w.Close()
		}()

		_, err = ibf.WriteTo(outfh)
		checkError(err)
	},
}

func init() {
	RootCmd.AddCommand(dump2mapCmd)

	dump2mapCmd.Flags().StringP("out-prefix", "o", "-", `out file prefix ("-" for stdout)`)
}
