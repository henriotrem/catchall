package main

import (
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/tidwall/sds"
	"github.com/tidwall/tinybtree"
	"github.com/tidwall/uhaha"
)

func main() {
	var conf uhaha.Config

	conf.Name = "catchall"
	conf.Version = "0.0.1"
	conf.InitialData = new(database)
	conf.Snapshot = snapshot
	conf.Restore = restore

	conf.AddWriteCommand("incr", cmdINCR)
	conf.AddWriteCommand("extract", cmdEXTRACT)
	conf.AddReadCommand("scan", cmdSCAN)
	conf.AddReadCommand("dbinfo", cmdDBINFO)

	uhaha.Main(conf)
}

type domain struct {
	name      string
	delivered int64
	bounced   int64
}

type database struct {
	current   string
	retrieved string
	periods   tinybtree.BTree
}

func getEpoch(now time.Time) string {
	return strconv.FormatInt(now.Unix()/30, 32)
}

func nextEpoch(current string) string {
	epoch, _ := strconv.ParseInt(current, 32, 64)
	return strconv.FormatInt(epoch+1, 32)
}

func previousEpoch(current string) string {
	epoch, _ := strconv.ParseInt(current, 32, 64)
	return strconv.FormatInt(epoch-1, 32)
}

func (db *database) getPeriod(epoch string, create bool) *tinybtree.BTree {
	v, _ := db.periods.Get(epoch)
	if v != nil {
		return v.(*tinybtree.BTree)
	}
	if !create {
		return nil
	}
	period := &tinybtree.BTree{}
	db.periods.Set(epoch, period)
	db.current = epoch
	if len(db.retrieved) == 0 {
		db.retrieved = previousEpoch(epoch)
	}
	return period
}

// INCR domain delivered bounced
// Increment the domain statistics for the current epoch
func cmdINCR(m uhaha.Machine, args []string) (interface{}, error) {
	data := m.Data().(*database)
	if len(args) < 4 {
		return nil, uhaha.ErrWrongNumArgs
	}

	var d *domain

	epoch := getEpoch(m.Now())
	name := string(args[1])
	delivered, _ := strconv.ParseInt(args[2], 10, 64)
	bounced, _ := strconv.ParseInt(args[3], 10, 64)

	p := data.getPeriod(epoch, true)

	v, existed := p.Get(name)

	if !existed {
		d = new(domain)
		d.name = name
		d.delivered = 0
		d.bounced = 0
	} else {
		d = v.(*domain)
	}

	d.delivered += delivered
	d.bounced += bounced
	p.Set(d.name, d)

	return "OK", nil
}

// EXTRACT epoch
// Extract the domain statistics and delete the previous period statistics
func cmdEXTRACT(m uhaha.Machine, args []string) (interface{}, error) {
	data := m.Data().(*database)
	if len(args) < 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	epoch := string(args[1])

	if epoch == data.current && epoch != nextEpoch(data.retrieved) {
		return nil, uhaha.ErrInvalid
	}

	p := data.getPeriod(epoch, false)
	arr := []string{}

	if p != nil {
		p.Scan(func(name string, v interface{}) bool {
			d := v.(*domain)
			arr = append(arr, d.name)
			arr = append(arr, fmt.Sprint(d.delivered))
			arr = append(arr, fmt.Sprint(d.bounced))
			return true
		})
	}

	data.periods.Delete(previousEpoch(epoch))
	data.retrieved = epoch

	return arr, nil
}

// SCAN epoch
// Retrieve the domain statistics for a specific period
func cmdSCAN(m uhaha.Machine, args []string) (interface{}, error) {
	data := m.Data().(*database)
	if len(args) < 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	epoch := string(args[1])

	p := data.getPeriod(epoch, false)
	arr := []string{}

	if p != nil {
		p.Scan(func(name string, v interface{}) bool {
			d := v.(*domain)
			arr = append(arr, d.name)
			arr = append(arr, fmt.Sprint(d.delivered))
			arr = append(arr, fmt.Sprint(d.bounced))
			return true
		})
	}

	return arr, nil
}

// DBINFO
// Retrieve the database information
func cmdDBINFO(m uhaha.Machine, args []string) (interface{}, error) {
	data := m.Data().(*database)
	if len(args) < 1 {
		return nil, uhaha.ErrWrongNumArgs
	}
	if len(data.current) == 0 {
		return "<nil> <nil>", nil
	}
	return data.current + " " + data.retrieved, nil
}

// #region -- SNAPSHOT & RESTORE

type snapDomain struct {
	epoch     string
	name      string
	delivered int64
	bounced   int64
}

type dbSnapshot struct {
	current   string
	retrieved string
	domains   []snapDomain
}

func (s *dbSnapshot) Persist(wr io.Writer) error {
	w := sds.NewWriter(wr)
	if err := w.WriteString(s.current); err != nil {
		return err
	}
	if err := w.WriteString(s.retrieved); err != nil {
		return err
	}
	if err := w.WriteUvarint(uint64(len(s.domains))); err != nil {
		return err
	}
	for _, d := range s.domains {
		if err := w.WriteString(d.epoch); err != nil {
			return err
		}
		if err := w.WriteString(d.name); err != nil {
			return err
		}
		if err := w.WriteInt64(d.delivered); err != nil {
			return err
		}
		if err := w.WriteInt64(d.bounced); err != nil {
			return err
		}
	}
	return w.Flush()
}

func (s *dbSnapshot) Done(path string) {
	if path != "" {
		// snapshot was a success.
	}
}

func snapshot(data interface{}) (uhaha.Snapshot, error) {
	db := data.(*database)
	snap := new(dbSnapshot)
	snap.current = db.current
	snap.retrieved = db.retrieved
	db.periods.Scan(func(epoch string, v interface{}) bool {
		period := v.(*tinybtree.BTree)
		period.Scan(func(name string, v interface{}) bool {
			d := v.(*domain)
			snap.domains = append(snap.domains, snapDomain{epoch, name, d.delivered, d.bounced})
			return true
		})
		return true
	})
	return snap, nil
}

func snapDomainObject(r *sds.Reader) (*domain, error) {
	d := new(domain)
	var err error
	d.name, err = r.ReadString()
	if err != nil {
		return nil, err
	}
	d.delivered, err = r.ReadInt64()
	if err != nil {
		return nil, err
	}
	d.bounced, err = r.ReadInt64()
	if err != nil {
		return nil, err
	}
	return d, nil
}

func restore(rd io.Reader) (interface{}, error) {
	db := new(database)
	r := sds.NewReader(rd)
	var err error
	if db.current, err = r.ReadString(); err != nil {
		return nil, err
	}
	if db.retrieved, err = r.ReadString(); err != nil {
		return nil, err
	}
	n, err := r.ReadUvarint()
	if err != nil {
		return nil, err
	}
	var period *tinybtree.BTree
	var lastEpoch string
	for i := uint64(0); i < n; i++ {
		epoch, err := r.ReadString()
		if err != nil {
			return nil, err
		}
		d, err := snapDomainObject(r)
		if err != nil {
			return nil, err
		}
		if epoch != lastEpoch {
			period = db.getPeriod(epoch, true)
			lastEpoch = epoch
		}
		period.Set(d.name, d)
	}
	return db, nil
}

// #endregion -- SNAPSHOT & RESTORE
