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
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"sync"

	"github.com/shenwei356/intintmap"

	"github.com/shenwei356/unikmer"
	"github.com/spf13/cobra"
)

// diffCmd represents
var diffCmd = &cobra.Command{
	Use:   "diff",
	Short: "set difference of multiple binary files",
	Long: `set difference of multiple binary files

Attentions:
  1. the 'canonical' flags of all files should be consistent.

Tips:
  1. Increasing threads number (-j/--threads) to accelerate computation,
     in cost of more memory occupation.

`,
	Run: func(cmd *cobra.Command, args []string) {
		opt := getOptions(cmd)

		var err error

		var files []string
		infileList := getFlagString(cmd, "infile-list")
		if infileList != "" {
			files, err = getListFromFile(infileList)
			checkError(err)
		} else {
			files = getFileList(args)
		}

		checkFiles(files)

		outFile := getFlagString(cmd, "out-prefix")
		sortKmers := getFlagBool(cmd, "sort")

		threads := opt.NumCPUs

		runtime.GOMAXPROCS(threads)

		// m := make(map[uint64]bool, mapInitSize)
		m := intintmap.New(mapInitSize, 0.4)

		var infh *bufio.Reader
		var r *os.File
		var reader *unikmer.Reader
		var kcode unikmer.KmerCode
		var k int = -1
		var canonical bool
		var ok bool
		var nfiles = len(files)

		// -----------------------------------------------------------------------

		file := files[0]
		if opt.Verbose {
			log.Infof("process file (%d/%d): %s", 1, nfiles, file)
		}

		// only one file given
		if len(files) == 1 {
			func() {
				infh, r, _, err = inStream(file)
				checkError(err)
				defer r.Close()

				reader, err = unikmer.NewReader(infh)
				checkError(err)

				k = reader.K

				if !isStdout(outFile) {
					outFile += extDataFile
				}
				outfh, gw, w, err := outStream(outFile, opt.Compress)
				checkError(err)
				defer func() {
					outfh.Flush()
					if gw != nil {
						gw.Close()
					}
					w.Close()
				}()

				var writer *unikmer.Writer
				var m2 []uint64

				if sortKmers {
					m2 = make([]uint64, 0, mapInitSize)
				} else {
					var mode uint32
					if opt.Compact {
						mode |= unikmer.UNIK_COMPACT
					}
					if reader.Flag&unikmer.UNIK_CANONICAL > 0 {
						mode |= unikmer.UNIK_CANONICAL
					}
					if sortKmers {
						mode |= unikmer.UNIK_SORTED
					}
					writer, err = unikmer.NewWriter(outfh, reader.K, mode)
					checkError(err)
				}

				m := make(map[uint64]struct{}, mapInitSize)
				for {
					kcode, err = reader.Read()
					if err != nil {
						if err == io.EOF {
							break
						}
						checkError(err)
					}

					if _, ok = m[kcode.Code]; !ok {
						m[kcode.Code] = struct{}{}
						if sortKmers {
							m2 = append(m2, kcode.Code)
						} else {
							writer.Write(kcode) // not need to check er
						}
					}
				}

				if sortKmers {
					var mode uint32
					if opt.Compact {
						mode |= unikmer.UNIK_COMPACT
					}
					if reader.Flag&unikmer.UNIK_CANONICAL > 0 {
						mode |= unikmer.UNIK_CANONICAL
					}
					mode |= unikmer.UNIK_SORTED
					writer, err = unikmer.NewWriter(outfh, reader.K, mode)
					checkError(err)

					writer.Number = int64(len(m2))

					if opt.Verbose {
						log.Infof("sort %d Kmers", len(m2))
					}
					sort.Sort(unikmer.CodeSlice(m2))
					if opt.Verbose {
						log.Infof("done sorting")
					}

					for _, code := range m2 {
						writer.Write(unikmer.KmerCode{Code: code, K: k})
					}
				}

				checkError(writer.Flush())
				if opt.Verbose {
					log.Infof("%d Kmers saved", len(m))
				}
			}()

			return
		}

		// -----------------------------------------------------------------------

		// > 1 files

		// read firstFile

		infh, r, _, err = inStream(file)
		checkError(err)

		reader, err = unikmer.NewReader(infh)
		checkError(err)

		k = reader.K
		canonical = reader.Flag&unikmer.UNIK_CANONICAL > 0

		for {
			kcode, err = reader.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				checkError(err)
			}

			// m[kcode.Code] = false
			m.Put(int64(kcode.Code), 10)
		}

		r.Close()

		if opt.Verbose {
			log.Infof("%d Kmers loaded", m.Size())
		}

		if m.Size() == 0 {
			if opt.Verbose {
				log.Infof("export Kmers")
			}

			if !isStdout(outFile) {
				outFile += extDataFile
			}
			outfh, gw, w, err := outStream(outFile, opt.Compress)
			checkError(err)
			defer func() {
				outfh.Flush()
				if gw != nil {
					gw.Close()
				}
				w.Close()
			}()

			var mode uint32
			if opt.Compact {
				mode |= unikmer.UNIK_COMPACT
			}
			if canonical {
				mode |= unikmer.UNIK_CANONICAL
			}
			if sortKmers {
				mode |= unikmer.UNIK_SORTED
			}

			writer, err := unikmer.NewWriter(outfh, k, mode)
			checkError(err)

			if sortKmers {
				writer.Number = 0
			}

			writer.Number = 0
			checkError(writer.WriteHeader())
			checkError(writer.Flush())

			if opt.Verbose {
				log.Infof("%d Kmers saved", 0)
			}
			return
		}
		// -----------------------------------------------------------------------

		done := make(chan int)

		toStop := make(chan int, threads+2)
		doneDone := make(chan int)
		go func() {
			<-toStop
			close(done)
			doneDone <- 1
		}()

		// ---------------

		type iFile struct {
			i    int
			file string
		}

		chFile := make(chan iFile, threads)
		doneSendFile := make(chan int)

		// maps := make(map[int]map[uint64]bool, threads)
		maps := make(map[int]*intintmap.Map, threads)
		maps[0] = m

		// clone maps
		if opt.Verbose {
			log.Infof("clone data for parallization")
		}
		var wg sync.WaitGroup
		for i := 1; i < opt.NumCPUs; i++ {
			wg.Add(1)
			go func(i int) {
				// m1 := make(map[uint64]bool, m.Size())
				m1 := intintmap.New(m.Size(), 0.4)
				for k := range m.Keys() {
					// m1[k] = false
					m1.Put(k, 10)
				}
				wg.Done()
				maps[i] = m1
			}(i)
		}
		wg.Wait()
		if opt.Verbose {
			log.Infof("done cloning data")
		}

		// -----------------------------------------------------------------------
		hasDiff := true
		var wgWorkers sync.WaitGroup
		for i := 0; i < opt.NumCPUs; i++ { // workers
			wgWorkers.Add(1)

			go func(i int) {
				defer func() {
					if opt.Verbose {
						log.Infof("worker %02d: finished with %d Kmers", i, maps[i].Size())
					}
					wgWorkers.Done()
				}()

				if opt.Verbose {
					log.Infof("worker %02d: started", i)
				}

				var ifile iFile
				var file string
				var infh *bufio.Reader
				var r *os.File
				var reader *unikmer.Reader
				var kcode unikmer.KmerCode
				var ok bool
				m1 := maps[i]
				for {
					ifile, ok = <-chFile
					if !ok {
						return
					}
					file = ifile.file

					select {
					case <-done:
						return
					default:
					}

					if opt.Verbose {
						log.Infof("worker %02d:  start processing file (%d/%d): %s", i, ifile.i+1, nfiles, file)
					}

					infh, r, _, err = inStream(file)
					checkError(err)

					reader, err = unikmer.NewReader(infh)
					checkError(err)

					if k != reader.K {
						checkError(fmt.Errorf("K (%d) of binary file '%s' not equal to previous K (%d)", reader.K, file, k))
					}

					if (reader.Flag&unikmer.UNIK_CANONICAL > 0) != canonical {
						checkError(fmt.Errorf(`'canonical' flags not consistent, please check with "unikmer stats"`))
					}

					for {
						kcode, err = reader.Read()
						if err != nil {
							if err == io.EOF {
								break
							}
							checkError(err)
						}

						// mark seen kmer
						// if _, ok = m1[kcode.Code]; ok { // slowest part
						// 	m1[kcode.Code] = true
						// }

						// if _, ok = m1.Get(int64(kcode.Code)); ok {
						// 	m1.Put(int64(kcode.Code), 20)
						// }

						m1.UpdateExistedKey(int64(kcode.Code), 20)

					}

					r.Close()

					// remove seen kmers
					// for code, mark = range m1 {
					// 	if mark {
					// 		delete(m1, code)
					// 	}
					// }
					for kv := range m1.Items() {
						if kv[1] > 10 {
							m1.Del(kv[0])
						}
					}

					if opt.Verbose {
						log.Infof("worker %02d: finish processing file (%d/%d): %s, %d Kmers remain", i, ifile.i+1, nfiles, file, m1.Size())
					}
					if m1.Size() == 0 {
						hasDiff = false
						toStop <- 1
						return
					}
				}
			}(i)
		}

		// send file
		go func() {
		SENDFILE:
			for i, file := range files[1:] {
				if file == files[0] {
					continue
				}
				select {
				case <-done:
					break SENDFILE
				default:
				}

				chFile <- iFile{i + 1, file}
			}
			close(chFile)

			doneSendFile <- 1
		}()

		<-doneSendFile
		wgWorkers.Wait()
		toStop <- 1
		<-doneDone

		// var m0 map[uint64]bool
		var m0 *intintmap.Map
		if !hasDiff {
			if opt.Verbose {
				log.Infof("no set difference found")
			}
			// return
		} else {
			var code int64
			for _, m := range maps {
				if m.Size() == 0 {
					m0 = m
					break
				}

				if m0 == nil {
					m0 = m
					continue
				}
				for code = range m0.Keys() {
					if _, ok = m.Get(code); !ok { // it's already been deleted in other m
						m0.Del(code) // so it should be deleted
					}
				}

				if m0.Size() == 0 {
					break
				}
			}

			if m0.Size() == 0 {
				if opt.Verbose {
					log.Warningf("no set difference found")
				}
				// return
			}
		}

		// -----------------------------------------------------------------------

		// output

		if opt.Verbose {
			log.Infof("export Kmers")
		}

		if !isStdout(outFile) {
			outFile += extDataFile
		}
		outfh, gw, w, err := outStream(outFile, opt.Compress)
		checkError(err)
		defer func() {
			outfh.Flush()
			if gw != nil {
				gw.Close()
			}
			w.Close()
		}()

		var mode uint32
		if opt.Compact {
			mode |= unikmer.UNIK_COMPACT
		}
		if canonical {
			mode |= unikmer.UNIK_CANONICAL
		}
		if sortKmers {
			mode |= unikmer.UNIK_SORTED
		}

		writer, err := unikmer.NewWriter(outfh, k, mode)
		checkError(err)

		if sortKmers {
			writer.Number = int64(m0.Size())
		}

		if m0.Size() == 0 {
			writer.Number = 0
			checkError(writer.WriteHeader())
		} else {
			if sortKmers {
				codes := make([]uint64, m0.Size())
				i := 0
				for code := range m0.Keys() {
					codes[i] = uint64(code)
					i++
				}
				if opt.Verbose {
					log.Infof("sort %d Kmers", len(codes))
				}
				sort.Sort(unikmer.CodeSlice(codes))
				if opt.Verbose {
					log.Infof("done sorting")
				}
				for _, code := range codes {
					writer.Write(unikmer.KmerCode{Code: code, K: k})
				}
			} else {
				for code := range m0.Keys() {
					writer.Write(unikmer.KmerCode{Code: uint64(code), K: k})
				}

			}
		}
		checkError(writer.Flush())
		if opt.Verbose {
			log.Infof("%d Kmers saved", m0.Size())
		}
	},
}

func init() {
	RootCmd.AddCommand(diffCmd)

	diffCmd.Flags().StringP("out-prefix", "o", "-", `out file prefix ("-" for stdout)`)
	diffCmd.Flags().BoolP("sort", "s", false, helpSort)
}
