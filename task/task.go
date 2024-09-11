package task

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"

	"dxlib/configuration"
	"dxlib/core"
	"dxlib/log"
	"dxlib/utils"
	"dxlib/utils/json"
)

const DXTaskDefaultAfterDelaySec = 1

type DXTaskOnExecute func(task *DXTask) error

type DXTask struct {
	NameId          string
	StartAt         string
	AfterDelaySec   int64
	OnExecute       DXTaskOnExecute
	Log             log.DXLog
	RuntimeIsActive bool
	Context         context.Context
	Cancel          context.CancelFunc
}

type DXTaskManager struct {
	Context           context.Context
	Cancel            context.CancelFunc
	Tasks             map[string]*DXTask
	ErrorGroup        *errgroup.Group
	ErrorGroupContext context.Context
}

func (am *DXTaskManager) NewTask(nameId string, startAt string, afterDelaySec int64, onExecute DXTaskOnExecute) (*DXTask, error) {
	ctx, cancel := context.WithCancel(am.Context)
	a := DXTask{
		NameId:        nameId,
		StartAt:       startAt,
		AfterDelaySec: afterDelaySec,
		OnExecute:     onExecute,
		Context:       ctx,
		Cancel:        cancel,
		Log:           log.NewLog(&log.Log, ctx, nameId),
	}
	am.Tasks[nameId] = &a
	return &a, nil
}

func (am *DXTaskManager) StartAll(errorGroup *errgroup.Group, errorGroupContext context.Context) error {
	am.ErrorGroup = errorGroup
	am.ErrorGroupContext = errorGroupContext

	am.ErrorGroup.Go(func() (err error) {
		<-am.ErrorGroupContext.Done()
		log.Log.Info(`Task Manager shutting down... start`)
		for _, v := range am.Tasks {
			vErr := v.StartShutdown()
			if (err == nil) && (vErr != nil) {
				err = vErr
			}
		}
		log.Log.Info(`Task Manager shutting down... done`)
		return nil
	})

	for _, v := range am.Tasks {
		err := v.StartAndWait(am.ErrorGroup)
		if err != nil {
			return err
		}
	}
	return nil
}

func (am *DXTaskManager) StopAll() (err error) {
	am.ErrorGroupContext.Done()
	err = am.ErrorGroup.Wait()
	return err
}

func (a *DXTask) ApplyConfigurations() (err error) {
	configurationTasks, ok := configuration.Manager.Configurations["tasks"]
	if !ok {
		err := log.Log.FatalAndCreateErrorf("Can not find configurationTasks 'tasks' needed to configure the tasks")
		return err
	}
	c := *configurationTasks.Data
	c1, ok := c[a.NameId].(utils.JSON)
	if !ok {
		return nil
	}

	tStartAt, ok := c1[`start_at`].(string)
	if ok {
		a.StartAt = tStartAt
	}
	tAfterDelaySec, err := json.GetNumber[int64](c1, `after_delay_sec`)
	if err == nil {
		a.AfterDelaySec = tAfterDelaySec
	}
	return err
}

func (a *DXTask) StartAndWait(errorGroup *errgroup.Group) error {
	if !a.RuntimeIsActive {
		err := a.ApplyConfigurations()
		if err != nil {
			return err
		}
		errorGroup.Go(func() (err error) {
			a.RuntimeIsActive = true
			log.Log.Infof("Starting task [%s] at %s... start", a.NameId, a.StartAt)
			switch a.StartAt {
			case "once":
				log.Log.Infof("Task %s at (%s): Starting task start", a.NameId, a.StartAt)
				err = a.OnExecute(a)
				log.Log.Infof("Task %s at (%s): Task done: %v", a.NameId, a.StartAt, err.Error())
				log.Log.Info("Start AfterDelay sleep...")
				time.Sleep(time.Duration(a.AfterDelaySec) * time.Second)
				log.Log.Info("Finish AfterDelay sleep...")
			case "always":
				inLoop := true
				var iterationIndex uint64 = 0
				for inLoop {
					log.Log.Infof("Task %s:%v at (%s): Execute task start", a.NameId, iterationIndex, a.StartAt)
					err = a.OnExecute(a)
					log.Log.Infof("Task %s:%v at (%s): Execute task done with result err=%v", a.NameId, iterationIndex, a.StartAt, err.Error())
					if err != nil {
						inLoop = false
					} else {
						log.Log.Infof("Task %s:%v at (%s): Start AfterDelay sleep... %v sec", a.NameId, iterationIndex, a.StartAt, a.AfterDelaySec)
						time.Sleep(time.Duration(a.AfterDelaySec) * time.Second)
						log.Log.Infof("Task %s:%v at (%s) Finish AfterDelay sleep...", a.NameId, iterationIndex, a.StartAt)
						select {
						case <-a.Context.Done():
							log.Log.Infof("Task %s:%v at (%s): Cancel triggered...", a.NameId, iterationIndex, a.StartAt)
							inLoop = false
						default:
						}
					}
					iterationIndex++
				}
			case "none":
			default:

			}
			a.RuntimeIsActive = false
			log.Log.Infof("Stopped task [%s] at %s... ", a.NameId, a.StartAt)
			return err
		})

	}
	return nil
}

func (a *DXTask) StartShutdown() (err error) {
	if a.RuntimeIsActive {
		log.Log.Infof("Shutdown api %s start...", a.NameId)
		a.Cancel()
		return err
	}
	return nil
}

var Manager DXTaskManager

func init() {
	ctx, cancel := context.WithCancel(core.RootContext)
	Manager = DXTaskManager{
		Context: ctx,
		Cancel:  cancel,
		Tasks:   map[string]*DXTask{},
	}
}
