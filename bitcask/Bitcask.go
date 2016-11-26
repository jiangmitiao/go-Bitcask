package bitcask

import (
	"path/filepath"
	"os"
	"fmt"
	"strings"
	"strconv"
	"encoding/json"
	"hash/crc32"
	"time"
	"io/ioutil"
	"io"
)

type Bitcask struct {
	TABLE       map[string]FileInfo
	THRESHOLD   int64
	Path        string
	Curr_active int
}

type FileInfo struct {
	Active_filename string
	Start           int64
	Length          int
}

func (b *Bitcask) Init(path string) {
	b.TABLE = make(map[string]FileInfo)
	b.THRESHOLD = 10000
	b.Path = path
	//b.Path = "./"
	if !FileExists(b.Path) {
		os.Mkdir(b.Path, 755)
		b.Curr_active = 1
	} else {
		max := 1
		filepath.Walk(b.Path, func(path string, f os.FileInfo, err error) error {
			//fmt.Println(path)
			if ! f.IsDir() {
				c, err := strconv.Atoi(strings.Split(f.Name(), ".")[0])
				if err == nil {
					if c > max {
						max = c
					}
				}
			}
			return nil
		})
		b.Curr_active = max
	}

}

func (b *Bitcask) InitWithOutPath() {
	b.Init("./bitcask/")
}

func (b *Bitcask) Put(key string, value interface{}) {
	key_size := len(key)
	value_size := 0
	if value != "" {
		bs, _ := json.Marshal(value)
		value_size = len(bs)
	}
	ts := time.Now()
	crc := crc32.ChecksumIEEE([]byte(fmt.Sprintf("%s,%s,%s,%s,%s", ts, key_size, value_size, key, value)))
	res := b.IoWrite([]interface{}{crc, ts, key_size, value_size, key, value})
	b.TABLE[key] = res
}

func (b *Bitcask) IoWrite(str []interface{}) FileInfo {
	//'''将记录序列化后写入文件'''
	active_filename := string(b.Path) + strconv.Itoa(b.Curr_active) + ".sst"
	//fmt.Println("filename : ", active_filename)
	f, err := os.OpenFile(active_filename, os.O_WRONLY, 755)
	if err != nil {
		//fmt.Println("唯一一次错误",err.Error())
		f, err = os.Create(active_filename)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(-1)
		}
	}
	if fi, _ := f.Stat(); fi.Size() >= b.THRESHOLD {
		f.Close()
		b.Curr_active += 1
		active_filename := string(b.Path) + strconv.Itoa(b.Curr_active) + ".sst"
		f, _ = os.Create(active_filename)
	}
	defer f.Close()

	data, _ := json.Marshal(str)
	n, _ := f.Seek(0, os.SEEK_END)
	length := len(data)
	_, err = f.WriteAt(data, n)

	return FileInfo{active_filename, n, length}
}

func (b *Bitcask) Get(key string) interface{} {
	record := b.GetRecord(key)
	if len(record) >= 5 {
		return record[5]
	}
	return ""
}

func (b *Bitcask) GetRecord(key string) []interface{} {
	data := []interface{}{}
	info, ok := b.TABLE[key]
	if ok {
		json.Unmarshal(b.IoRead(info), &data)
	}
	return data
}

func (b *Bitcask) IoRead(fileinfo FileInfo) []byte {
	f, err := os.OpenFile(fileinfo.Active_filename, os.O_RDONLY, 755)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer f.Close()
	bs := make([]byte, fileinfo.Length)
	f.ReadAt(bs, fileinfo.Start)
	//fmt.Println(string(bs))
	return bs
}

func (b *Bitcask) Delete(key string) {
	b.Put(key, "")
}

func (b *Bitcask) Update(key string, value interface{}) {
	b.Put(key, value)
}

func (b *Bitcask) LoadData(path string) {
	//'''指定路径,加载该文件夹目录下所有数据文件'''
	filepath.Walk(path, func(path string, f os.FileInfo, err error) error {
		if ! f.IsDir() {
			_, err := strconv.Atoi(strings.Split(f.Name(), ".")[0])
			if err == nil {
				if len(strings.Split(f.Name(), ".")) == 2 && strings.Split(f.Name(), ".")[1] == "sst" {
					b.LoadFile(path)
				}
			}
		}
		return nil
	})
}

func (b *Bitcask) LoadFile(fileName string) error {
	f, err := os.OpenFile(fileName, os.O_RDONLY, 644)
	if err == nil {
		defer f.Close()
		bs, _ := ioutil.ReadAll(f)

		datastrs := strings.Split(string(bs), "]")
		start := int64(0)
		length := 0
		for i := 0; i < len(datastrs) - 1; i++ {
			data := []interface{}{}
			json.Unmarshal([]byte(datastrs[i] + "]"), &data)

			length = len([]byte(datastrs[i])) + 1
			if _, ok := b.TABLE[data[4].(string)]; ok {
				//看时间
				time.Now()
				ts1 := data[1].(time.Time)
				ts2 := b.GetRecord(data[4].(string))[1].(time.Time)
				if ts1.Unix() > ts2.Unix() {
					b.TABLE[data[4].(string)] = FileInfo{fileName, start, length}
				}
			} else {
				b.TABLE[data[4].(string)] = FileInfo{fileName, start, length}
			}
			start += int64(length)
		}
		return nil
	} else {
		fmt.Println("error")
		return err
	}
}

func (b Bitcask) ListKeys() []string {
	keys := []string{}
	for k, _ := range b.TABLE {
		keys = append(keys, k)
	}
	return keys
}

func (b *Bitcask) Merge() error {
	tmpPath, e := ioutil.TempDir(b.Path, ".mergeTmp")
	if e != nil {
		return e
	} else {
		//fmt.Println(tmpPath)
		bTmp := Bitcask{}
		bTmp.Init(tmpPath + "/")
		for k := range b.TABLE {
			//top the world
			bTmp.Put(k, b.Get(k))
			b.TABLE[k] = bTmp.TABLE[k]

			_, fn := filepath.Split(b.TABLE[k].Active_filename)
			b.TABLE[k] = FileInfo{
				b.Path + fn,
				b.TABLE[k].Start,
				b.TABLE[k].Length,
			}
		}
		//delete the old
		filepath.Walk(b.Path, func(path string, info os.FileInfo, err error) error {
			if !info.IsDir() {
				//fmt.Println("remove", path)
				os.Remove(path)
			} else if strings.Contains(path, ".merge") {
				return filepath.SkipDir
			}
			return nil
		})
		//copy the new
		filepath.Walk(bTmp.Path, func(path string, info os.FileInfo, err error) error {
			if !info.IsDir() {
				src, _ := os.Open(path)
				//fmt.Println(b.Path + info.Name())
				dst, _ := os.Create(b.Path + info.Name())
				defer src.Close()
				defer dst.Close()
				_, e := io.Copy(dst, src)
				if e != nil {
					fmt.Println(e.Error())
					return e
				}
			}
			return nil
		})
		//remove tmp
		os.RemoveAll(tmpPath)
		return nil
	}
}

func (b Bitcask) TouchHint() error {
	//stop the world
	//delete the old
	strs, e := filepath.Glob(b.Path + "*.hit")
	if e != nil {
		fmt.Println(e.Error())
		return e
	}
	for _, str := range strs {
		os.Remove(str)
	}
	//touch the new
	f, _ := os.Create(b.Path + "1.hit")
	defer f.Close()
	//for k,v := range b.TABLE{
	//	datas := struct {
	//		K string
	//		V FileInfo
	//	}{k,v}
	//	data, _ := json.Marshal(datas)
	//
	//	_, e = f.Write(data)
	//}
	data, _ := json.Marshal(b.TABLE)
	_, e = f.Write(data)
	return e
}

func (b *Bitcask) LoadDataWithHint() error {
	strs, e := filepath.Glob(b.Path + "*.hit")
	if e != nil {
		fmt.Println(e.Error())
		return e
	}
	for _, str := range strs {
		f, err := os.OpenFile(str, os.O_RDONLY, 644)
		if err == nil {
			defer f.Close()
			bs, _ := ioutil.ReadAll(f)
			data := map[string]FileInfo{}
			json.Unmarshal(bs, &data)
			for k, v := range data {
				b.TABLE[k] = v
			}
		}
	}
	return nil
}

func FileExists(name string) bool {
	_, err := os.Stat(name)
	return !os.IsNotExist(err)
}

func main() {
	b := Bitcask{}
	b.InitWithOutPath()
	b.LoadDataWithHint()
	//fmt.Println(b)
	//
	//b.Put("1", 123)
	//b.Put("1", 12)
	//b.Put("2", "zz")
	//b.Put("2", 223)
	//b.Put("3", 323)
	//b.Put("4", 423)
	//
	//
	//fmt.Println(b.ListKeys())

	//b.Merge()
	fmt.Println(b.TABLE)
	fmt.Println(b.Get("2"))
	b.TouchHint()


	//for i := 0; i < 1000; i++ {
	//	b.Put(strconv.Itoa(i), i)
	//}

	//b.LoadData("./bitcask/")
	//fmt.Println(b.TABLE)
	//fmt.Println(b.Get("0"))
	//fmt.Println(b.Get("99"))
	//fmt.Println(b.Get("288"))
	//fmt.Println(b.Get("309"))
	//fmt.Println(b.Get("999"))


	//fmt.Println(b.Get("164"))

	//fmt.Println(b.Get("999"))
	//fmt.Println(b.Curr_active)
}

