package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Task struct {
	ID         int64
	CreatedAt  time.Time // время создания
	FinishedAt time.Time // время выполнения
	Result     error
}

type Counter struct {
	count int64
}

func (c *Counter) Next() int64 {
	return atomic.AddInt64(&c.count, 1)
}

var debugLog *log.Logger

func main() {

	debug := os.Getenv("DEBUG")
	debugLog = log.New(os.Stdout, "DEBUG: ", log.Ldate|log.Ltime|log.Lshortfile)

	var wg sync.WaitGroup

	counter := &Counter{}

	taskCreator := func(chanTask chan<- Task) {
		wg.Add(1)
		defer wg.Done()
		for i := 0; i < 10; i++ { // ограничиваем количество тасков до 10
			ft := time.Now()
			var err error = nil
			newId := counter.Next()
			chanTask <- Task{CreatedAt: ft, ID: newId, Result: err} // передаем таск на выполнение
		}
		close(chanTask) // закрываем канал после создания всех задач
	}
	chanTaskList := make(chan Task, 10)

	go taskCreator(chanTaskList)
	time.Sleep(time.Millisecond * 1000)
	task_worker := func(task Task) Task {
		wg.Add(1)
		defer wg.Done()
		var err error = nil
		halfHour := 30 * time.Minute
		task.FinishedAt = task.CreatedAt.Add(halfHour)
		if task.ID%2 > 0 { // вот такое условие появления ошибочных тасков
			err = errors.New("Some error")
		}
		task.Result = err

		time.Sleep(time.Millisecond * 150)

		return task
	}

	doneTasks := map[int]Task{}
	undoneTasks := map[int]Task{}

	tasksorter := func(task Task) {
		if task.Result == nil {
			taskNew := task
			doneTasks[int(taskNew.ID)] = taskNew
		} else {
			taskNew := task
			undoneTasks[int(taskNew.ID)] = taskNew
		}
	}
	go func() {
		wg.Add(1)
		defer wg.Done()
		for task := range chanTaskList {
			task = task_worker(task)
			if debug == "true" {
				if task.Result == nil {
					debugLog.Println("Task", task.ID, "NIL")
				} else {
					debugLog.Println("Task", task.ID, task.Result)
				}
			}
			go tasksorter(task)
		}
	}()

	time.Sleep(time.Millisecond * 10)
	wg.Wait() // ожидание завершения всех горутин, которые отправляют данные в канал
	log.Println("Done tasks:")
	for _, task := range doneTasks {
		createdAt := task.CreatedAt.Format("2006-01-02 15:04:05")
		finishedAt := task.FinishedAt.Format("2006-01-02 15:04:05")
		str := fmt.Sprintf("id: %d, Created: %s, Finished: %s", task.ID, createdAt, finishedAt)
		log.Println(str)
	}
	log.Println("Errors:")
	for _, task := range undoneTasks {
		createdAt := task.CreatedAt.Format("2006-01-02 15:04:05")
		finishedAt := task.FinishedAt.Format("2006-01-02 15:04:05")
		errMsg := task.Result.Error()
		str := fmt.Sprintf("id: %d, Created: %s, Finished: %s, %s", task.ID, createdAt, finishedAt, errMsg)
		log.Println(str)
	}
}
