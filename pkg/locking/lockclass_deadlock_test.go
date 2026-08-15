//go:build deadlock

/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package locking

import (
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	godeadlock "github.com/sasha-s/go-deadlock"
	"gotest.tools/v3/assert"
)

// annotationClass maps the name a class carries in the static annotations of locking.go to the
// class of the runtime checker. Only the manager differs: the runtime name is qualified with its
// package, which reads better in a report than in an annotation.
var annotationClass = map[string]Class{
	"ClusterContext":   ClassClusterContext,
	"PartitionContext": ClassPartitionContext,
	"Application":      ClassApplication,
	"Queue":            ClassQueue,
	"Node":             ClassNode,
	"UGMManager":       ClassUGMManager,
	"UserTracker":      ClassUserTracker,
	"GroupTracker":     ClassGroupTracker,
}

// orderAnnotations parses the order the static analyzer reads out of the package doc of
// locking.go: the edges, the hierarchical classes and the classes whose same class rule is
// withheld. The file is parsed rather than the annotations being repeated here, so that the
// test compares the two declarations instead of comparing a copy of one with the other.
func orderAnnotations(t *testing.T) (edges [][2]Class, hier, withheld map[Class]bool) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "locking.go", nil, parser.ParseComments)
	assert.NilError(t, err, "parsing the file that declares the order")
	assert.Assert(t, file.Doc != nil, "locking.go must carry the annotated order in its package doc")

	class := func(name string) Class {
		t.Helper()
		c, ok := annotationClass[name]
		assert.Assert(t, ok, "the annotations name a class the runtime checker does not have: %s", name)
		return c
	}
	hier = make(map[Class]bool)
	withheld = make(map[Class]bool)
	for _, c := range file.Doc.List {
		text := strings.TrimSpace(strings.TrimPrefix(c.Text, "//"))
		switch {
		case strings.HasPrefix(text, "+lockorder:"):
			before, after, ok := strings.Cut(strings.TrimPrefix(text, "+lockorder:"), "<")
			assert.Assert(t, ok, "an order annotation must read \"A < B\": %s", text)
			edges = append(edges, [2]Class{class(strings.TrimSpace(before)), class(strings.TrimSpace(after))})
		case strings.HasPrefix(text, "+lockhierarchical:"):
			hier[class(strings.TrimSpace(strings.TrimPrefix(text, "+lockhierarchical:")))] = true
		case strings.HasPrefix(text, "+lockorderwithheld:"):
			withheld[class(strings.TrimSpace(strings.TrimPrefix(text, "+lockorderwithheld:")))] = true
		}
	}
	return edges, hier, withheld
}

// classSet turns the flag array of the checker into a set, so both sides of a comparison are
// written the same way.
func classSet(flags [numClasses]bool) map[Class]bool {
	set := make(map[Class]bool)
	for c := Class(1); c < numClasses; c++ {
		if flags[c] {
			set[c] = true
		}
	}
	return set
}

// edgeStrings renders a set of edges as sorted text, which makes a mismatch readable.
func edgeStrings(edges [][2]Class) []string {
	out := make([]string, 0, len(edges))
	for _, e := range edges {
		out = append(out, e[0].String()+" < "+e[1].String())
	}
	sort.Strings(out)
	return out
}

// classIdent maps the name of the constant that names a class to the class itself, so that a
// "SetClass(locking.ClassNode)" call can be read back out of the source without type information.
var classIdent = func() map[string]Class {
	idents := make(map[string]Class, len(annotationClass))
	for name, c := range annotationClass {
		idents["Class"+name] = c
	}
	return idents
}()

// moduleRoot is the root of the module, relative to this package.
const moduleRoot = "../.."

// classWiring walks the module for the two halves of a class declaration: the "+lockclass"
// annotation the type carries, and the SetClass call its constructor makes. Both are collected as
// the set of classes of a directory, which is as far as a parse without type information can tie a
// call to the type it is made on, and is far enough: no package annotates one class and registers
// another. A set rather than a list because a class is annotated once and registered by every
// constructor of its type, of which the cluster context has two.
//
// Only the files of a normal build are read. The canary of this package sits behind its own build
// tag and declares classes that deliberately belong to it alone, and test files build their own
// objects, neither of which says anything about how the scheduler is wired.
func classWiring(t *testing.T) (annotated, registered map[string][]string) {
	t.Helper()
	annotated = make(map[string][]string)
	registered = make(map[string][]string)

	add := func(into map[string][]string, dir string, c Class) {
		name := c.String()
		for _, have := range into[dir] {
			if have == name {
				return
			}
		}
		into[dir] = append(into[dir], name)
	}
	annotatedClass := func(name string) Class {
		t.Helper()
		c, ok := annotationClass[name]
		assert.Assert(t, ok, "a type is annotated with a class the runtime checker does not have: %s", name)
		return c
	}
	registeredClass := func(name string) Class {
		t.Helper()
		c, ok := classIdent[name]
		assert.Assert(t, ok, "SetClass is called with something that is not a class constant: %s", name)
		return c
	}
	err := filepath.WalkDir(moduleRoot, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			// the root is reached as "..", so it is only the directories below it that a
			// leading dot marks as none of the module's own source
			if path != moduleRoot && strings.HasPrefix(entry.Name(), ".") {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		dir := filepath.Dir(path)
		built, err := build.Default.MatchFile(dir, entry.Name())
		if err != nil || !built {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		dir, err = filepath.Rel(moduleRoot, dir)
		if err != nil {
			return err
		}
		for _, group := range file.Comments {
			for _, c := range group.List {
				text := strings.TrimSpace(strings.TrimPrefix(c.Text, "//"))
				if name, ok := strings.CutPrefix(text, "+lockclass:"); ok {
					add(annotated, dir, annotatedClass(strings.TrimSpace(name)))
				}
			}
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			fun, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || fun.Sel.Name != "SetClass" || len(call.Args) != 1 {
				return true
			}
			var name string
			switch arg := call.Args[0].(type) {
			case *ast.SelectorExpr:
				name = arg.Sel.Name
			case *ast.Ident:
				name = arg.Name
			}
			add(registered, dir, registeredClass(name))
			return true
		})
		return nil
	})
	assert.NilError(t, err, "walking the module for the class declarations")
	for _, classes := range annotated {
		sort.Strings(classes)
	}
	for _, classes := range registered {
		sort.Strings(classes)
	}
	return annotated, registered
}

// TestLockOrderAnnotationsMatchRuntime is the guard on the one thing that keeps the runtime
// checker in this package and the static analysers of the "vetlock" make target enforcing the same
// rule: the order the annotations of locking.go declare and the order enforced below must be the
// same graph, and the class a type is annotated with must be the class its constructor registers.
// Either half can be edited on its own, and a drift between them is silent: the static side would
// then pass the code the runtime side rejects, or the other way round, with no build ever failing.
func TestLockOrderAnnotationsMatchRuntime(t *testing.T) {
	annotatedEdges, annotatedHier, annotatedWithheld := orderAnnotations(t)

	assert.DeepEqual(t, edgeStrings(annotatedEdges), edgeStrings(declaredOrder))
	assert.DeepEqual(t, annotatedHier, classSet(hierarchical))
	assert.DeepEqual(t, annotatedWithheld, classSet(sameClassWithheld))

	// A class the annotations have no name for cannot be kept in step, so adding one to the
	// checker must fail here rather than quietly leave the static side without it.
	for c := Class(1); c < numClasses; c++ {
		named := false
		for _, mapped := range annotationClass {
			if mapped == c {
				named = true
				break
			}
		}
		assert.Assert(t, named, "class %s has no name in the annotations", c)
	}

	// The order above only binds the classes to each other. What binds a class to a lock is the
	// SetClass call in the constructor, and an annotated type whose constructor forgets it, or
	// registers a different class, leaves the runtime check reading an order the annotations do
	// not describe.
	annotated, registered := classWiring(t)
	assert.DeepEqual(t, annotated, registered)
}

// resetClassCheck puts the checker back into a known state: no goroutine holds anything and no
// pair has been reported.
//
// It also takes over the go-deadlock report callback for the duration of the test. That callback
// exits the process when DEADLOCK_EXIT is set, which is what "make test" sets: a test that trips
// the checker on purpose would take the whole test binary down with it. The tests assert on the
// reported pairs instead, and the real callback is restored afterwards rather than the process
// wide testing mode being switched on for every test that runs after this one.
func resetClassCheck(t *testing.T, enabled bool) {
	t.Helper()
	resetClassOrderState()
	classOrderEnabled.Store(enabled)

	realReport := godeadlock.Opts.OnPotentialDeadlock
	godeadlock.Opts.OnPotentialDeadlock = func() {}
	t.Cleanup(func() {
		classOrderEnabled.Store(false)
		godeadlock.Opts.OnPotentialDeadlock = realReport
	})
}

// captureReports takes the shared report buffer over for the duration of the test and returns a
// reader of what was written to it. The buffer is the guarded one this package installs, the
// go-deadlock watchdog of another test can be writing to it at the same time.
func captureReports(t *testing.T) func() string {
	t.Helper()
	buf := &errorBuf{}
	realBuf := godeadlock.Opts.LogBuf
	godeadlock.Opts.LogBuf = buf
	t.Cleanup(func() { godeadlock.Opts.LogBuf = realBuf })
	return func() string {
		buf.Lock()
		defer buf.Unlock()
		return buf.data
	}
}

// tripped reports whether the given ordered pair has been reported by the checker. The tests use
// this rather than IsDeadlockDetected: the detected flag is shared with the go-deadlock watchdog,
// which fires asynchronously from other tests in this same binary.
func tripped(heldClass, acquired Class) bool {
	return reported[heldClass][acquired].Load()
}

// anyTripped reports whether the checker reported any pair at all.
func anyTripped() bool {
	for i := Class(0); i < numClasses; i++ {
		for j := Class(0); j < numClasses; j++ {
			if reported[i][j].Load() {
				return true
			}
		}
	}
	return false
}

func classed(c Class) *RWMutex {
	m := &RWMutex{}
	m.SetClass(c)
	return m
}

// TestClassOrderUpwardIsViolation takes a lock lower in the order and then one above it.
func TestClassOrderUpwardIsViolation(t *testing.T) {
	resetClassCheck(t, true)
	app := classed(ClassApplication)
	partition := classed(ClassPartitionContext)

	app.Lock()
	assert.Assert(t, !tripped(ClassApplication, ClassPartitionContext), "holding the application lock alone is not a violation")
	partition.Lock()
	assert.Assert(t, tripped(ClassApplication, ClassPartitionContext), "taking the partition lock under the application lock must be reported")
	partition.Unlock()
	app.Unlock()
}

// TestClassOrderDownwardIsAllowed walks the documented order from the top down.
func TestClassOrderDownwardIsAllowed(t *testing.T) {
	resetClassCheck(t, true)
	cluster := classed(ClassClusterContext)
	partition := classed(ClassPartitionContext)
	app := classed(ClassApplication)
	node := classed(ClassNode)

	cluster.Lock()
	partition.Lock()
	app.Lock()
	node.RLock()
	assert.Assert(t, !anyTripped(), "the documented order must not be reported")
	node.RUnlock()
	app.Unlock()
	partition.Unlock()
	cluster.Unlock()
}

// TestClassOrderTransitive checks the closure: the direct edges do not relate a user tracker to the
// cluster context but the closure does.
func TestClassOrderTransitive(t *testing.T) {
	resetClassCheck(t, true)
	tracker := classed(ClassUserTracker)
	cluster := classed(ClassClusterContext)

	tracker.Lock()
	cluster.Lock()
	assert.Assert(t, tripped(ClassUserTracker, ClassClusterContext), "cluster context under a user tracker is an upward acquisition")
	cluster.Unlock()
	tracker.Unlock()
}

// TestClassOrderUnrelatedPairSilent uses two classes the document does not order against each
// other. Guessing an order there would only produce noise.
func TestClassOrderUnrelatedPairSilent(t *testing.T) {
	resetClassCheck(t, true)
	node := classed(ClassNode)
	queue := classed(ClassQueue)

	node.Lock()
	queue.Lock()
	assert.Assert(t, !anyTripped(), "node and queue are not ordered against each other")
	queue.Unlock()
	node.Unlock()

	// and the other way round
	resetClassCheck(t, true)
	queue.Lock()
	node.Lock()
	assert.Assert(t, !anyTripped(), "node and queue are not ordered against each other")
	node.Unlock()
	queue.Unlock()
}

// TestClassOrderClassless leaves the locks without a class: they must not be tracked at all.
func TestClassOrderClassless(t *testing.T) {
	resetClassCheck(t, true)
	one := &RWMutex{}
	two := &RWMutex{}

	one.Lock()
	two.Lock()
	assert.Assert(t, !anyTripped(), "locks without a class are not ordered")
	two.Unlock()
	one.Unlock()
}

// TestClassOrderSameClass nests two locks of the same class. That is a violation unless the class
// is a hierarchy or its same class rule is withheld. The report has to name the rule that is
// broken: there is no edge between a class and itself to quote.
func TestClassOrderSameClass(t *testing.T) {
	resetClassCheck(t, true)
	reports := captureReports(t)
	first := classed(ClassNode)
	second := classed(ClassNode)

	first.Lock()
	second.Lock()
	assert.Assert(t, tripped(ClassNode, ClassNode), "nesting two node locks must be reported")
	assert.Assert(t, strings.Contains(reports(), "two locks of one class must not nest, Node is not declared hierarchical"),
		"the same class report must name the rule rather than an order: %s", reports())
	second.Unlock()
	first.Unlock()
}

// TestClassOrderSameClassWithheld covers the classes whose same class rule is not enforced yet.
func TestClassOrderSameClassWithheld(t *testing.T) {
	resetClassCheck(t, true)
	first := classed(ClassApplication)
	second := classed(ClassApplication)

	first.Lock()
	second.Lock()
	assert.Assert(t, !anyTripped(), "the application same class rule is withheld in this version")
	second.Unlock()
	first.Unlock()
}

// TestClassOrderHierarchyExempt nests two queue locks, the queue hierarchy is exempt.
func TestClassOrderHierarchyExempt(t *testing.T) {
	resetClassCheck(t, true)
	parent := classed(ClassQueue)
	child := classed(ClassQueue)

	parent.Lock()
	child.Lock()
	assert.Assert(t, !anyTripped(), "the queue hierarchy is exempt from the same class rule")
	child.Unlock()
	parent.Unlock()
}

// TestClassOrderDisabled repeats the violation case with the checker turned off.
func TestClassOrderDisabled(t *testing.T) {
	resetClassCheck(t, false)
	app := classed(ClassApplication)
	partition := classed(ClassPartitionContext)

	app.Lock()
	partition.Lock()
	assert.Assert(t, !anyTripped(), "nothing is checked while the switch is off")
	partition.Unlock()
	app.Unlock()

	// and nothing was tracked either
	total := 0
	for i := range heldShards {
		heldShards[i].lock.Lock()
		total += len(heldShards[i].goroutines)
		heldShards[i].lock.Unlock()
	}
	assert.Equal(t, 0, total, "no goroutine state may be kept while the switch is off")
}

// TestClassOrderPerGoroutine holds a low lock on one goroutine while another goroutine takes a
// high one: the two must not see each other's held set.
func TestClassOrderPerGoroutine(t *testing.T) {
	resetClassCheck(t, true)
	app := classed(ClassApplication)
	partition := classed(ClassPartitionContext)

	held := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		app.Lock()
		close(held)
		<-release
		app.Unlock()
	}()

	<-held
	// this goroutine holds nothing, so the partition lock is fine even though the other goroutine
	// is sitting on an application lock
	partition.Lock()
	assert.Assert(t, !anyTripped(), "held classes must not leak between goroutines")
	partition.Unlock()
	close(release)
	<-done
}

// TestClassOrderUnlockOutOfOrder releases the locks in acquisition order rather than in reverse:
// the multiset must still end up empty.
func TestClassOrderUnlockOutOfOrder(t *testing.T) {
	resetClassCheck(t, true)
	cluster := classed(ClassClusterContext)
	partition := classed(ClassPartitionContext)
	app := classed(ClassApplication)

	cluster.Lock()
	partition.Lock()
	app.Lock()
	// release in the same order they were taken
	cluster.Unlock()
	partition.Unlock()
	app.Unlock()
	assert.Assert(t, !anyTripped(), "the documented order must not be reported")

	total := 0
	for i := range heldShards {
		heldShards[i].lock.Lock()
		total += len(heldShards[i].goroutines)
		heldShards[i].lock.Unlock()
	}
	assert.Equal(t, 0, total, "the goroutine must be dropped once it holds nothing")
}

// TestClassOrderReportOnce trips the same pair twice and checks only one report is produced.
func TestClassOrderReportOnce(t *testing.T) {
	resetClassCheck(t, true)
	app := classed(ClassApplication)
	partition := classed(ClassPartitionContext)

	app.Lock()
	partition.Lock()
	partition.Unlock() //nolint:staticcheck // SA2001: the critical section is empty on purpose, the acquisition is what is checked
	app.Unlock()
	assert.Assert(t, tripped(ClassApplication, ClassPartitionContext), "the pair is marked as reported")

	reportCount.Store(0)
	app.Lock()
	partition.Lock()
	assert.Equal(t, int32(0), reportCount.Load(), "the same pair must only be reported once")
	partition.Unlock()
	app.Unlock()
}

// TestClassOrderReadLocksOrder checks the read side is ordered the same way as the write side.
func TestClassOrderReadLocksOrder(t *testing.T) {
	resetClassCheck(t, true)
	tracker := classed(ClassGroupTracker)
	manager := classed(ClassUGMManager)

	tracker.RLock()
	manager.RLock()
	assert.Assert(t, tripped(ClassGroupTracker, ClassUGMManager), "the manager read lock under a tracker read lock is upward")
	manager.RUnlock()
	tracker.RUnlock()
}

// TestClassOrderConcurrentNoRace hammers the checker from several goroutines, it is here to be run
// under the race detector.
func TestClassOrderConcurrentNoRace(t *testing.T) {
	resetClassCheck(t, true)
	manager := classed(ClassUGMManager)
	tracker := classed(ClassUserTracker)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				manager.Lock()
				tracker.Lock()
				tracker.Unlock() //nolint:staticcheck // SA2001: the critical section is empty on purpose, the acquisition is what is checked
				manager.Unlock()
			}
		}()
	}
	wg.Wait()
	assert.Assert(t, !anyTripped(), "the documented order must not be reported")
}
