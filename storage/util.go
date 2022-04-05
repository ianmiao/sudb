package storage

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
)

func getMemTableWALDir(dbDir string) string {
	return filepath.Join(dbDir, MemTableWALPath)
}

func getSSTableKeyIndexDir(dbDir string) string {
	return filepath.Join(dbDir, SSTableKeyIndexPath)
}

func getSSTableDataDir(dbDir string) string {
	return filepath.Join(dbDir, SSTableDataPath)
}

func getSSTableTmpKeyIndexDir(dbDir string) string {
	return filepath.Join(dbDir, SSTableTmpKeyIndexPath)
}

func getSSTableTmpDataDir(dbDir string) string {
	return filepath.Join(dbDir, SSTableTmpDataPath)
}

func getLevelSSTableKeyIndexDir(dbDir string, level int) string {
	return filepath.Join(dbDir, SSTableKeyIndexPath, strconv.Itoa(level))
}

func getLevelSSTableDataDir(dbDir string, level int) string {
	return filepath.Join(dbDir, SSTableDataPath, strconv.Itoa(level))
}

func mkdirIfNotExist(paths ...string) error {
	for _, path := range paths {
		if err := os.MkdirAll(path, 0700); err != nil {
			return err
		}
	}
	return nil
}

func cleanDirs(dirs ...string) error {
	for _, dir := range dirs {
		files, err := ioutil.ReadDir(dir)
		if err != nil {
			return err
		}

		for _, file := range files {
			if err := os.RemoveAll(filepath.Join(dir, file.Name())); err != nil {
				return err
			}
		}
	}

	return nil
}

func ssTableNames(ssTables ...*SSTable) []string {
	names := make([]string, len(ssTables))
	for idx, ssTable := range ssTables {
		names[idx] = ssTable.fileName
	}
	return names
}
