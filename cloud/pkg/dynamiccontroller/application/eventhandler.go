/*
Copyright 2021 The KubeEdge Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package application

import (
	"context"
	"fmt"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/kubeedge/kubeedge/cloud/pkg/common/client"
	genericinformers "github.com/kubeedge/kubeedge/cloud/pkg/common/informers"
	"github.com/kubeedge/kubeedge/cloud/pkg/common/messagelayer"
	"github.com/kubeedge/kubeedge/pkg/metaserver/util"
)

// HandlerCenter is used to prepare corresponding CommonResourceEventHandler for listener
// CommonResourceEventHandler then will send to the listener the objs it is interested in,
// including subsequent changes (watch event)
type HandlerCenter interface {
	AddListener(s *SelectorListener) error
	DeleteListener(s *SelectorListener)
	ForResource(gvr schema.GroupVersionResource) *CommonResourceEventHandler
	GetListenersForNode(nodeName string) map[string]*SelectorListener
}

type handlerCenter struct {
	// handlerLock used to protect the handlers map
	handlerLock                  sync.Mutex
	listenerManager              *listenerManager
	handlers                     map[schema.GroupVersionResource]*CommonResourceEventHandler
	dynamicSharedInformerFactory dynamicinformer.DynamicSharedInformerFactory
	messageLayer                 messagelayer.MessageLayer
}

func NewHandlerCenter(informerFactory dynamicinformer.DynamicSharedInformerFactory) HandlerCenter {
	handlers := make(map[schema.GroupVersionResource]*CommonResourceEventHandler)

	c := handlerCenter{
		listenerManager:              newListenerManager(),
		handlers:                     handlers,
		dynamicSharedInformerFactory: informerFactory,
		messageLayer:                 messagelayer.DynamicControllerMessageLayer(),
	}
	return &c
}

func (c *handlerCenter) ForResource(gvr schema.GroupVersionResource) *CommonResourceEventHandler {
	c.handlerLock.Lock()
	defer c.handlerLock.Unlock()

	if handler, ok := c.handlers[gvr]; ok {
		return handler
	}

	klog.Infof("[metaserver/HandlerCenter] prepare a new resourceEventHandler(%v)", gvr)

	handler := NewCommonResourceEventHandler(gvr, c.listenerManager, c.messageLayer)
	c.handlers[gvr] = handler

	return handler
}

// AddListener dispatch listeners to corresponding CommonResourceEventHandler according it's gvr
func (c *handlerCenter) AddListener(s *SelectorListener) error {
	return c.ForResource(s.gvr).AddListener(s)
}

func (c *handlerCenter) DeleteListener(s *SelectorListener) {
	c.handlerLock.Lock()
	c.handlers[s.gvr].DeleteListener(s)
	c.handlerLock.Unlock()
}

func (c *handlerCenter) GetListenersForNode(nodeName string) map[string]*SelectorListener {
	return c.listenerManager.GetListenersForNode(nodeName)
}

// CommonResourceEventHandler can be used by configmapManager and podManager
type CommonResourceEventHandler struct {
	events chan watch.Event

	//TODO: num of listeners is proportional to the number of request, need reduce.
	listenerManager *listenerManager
	messageLayer    messagelayer.MessageLayer
	gvr             schema.GroupVersionResource
	// stopCh is used to stop the DRA custom informer goroutine
	stopCh chan struct{}
	// informer is set only for non-DRA resources (original path).
	// For DRA resources this stays nil; AddListener uses draInformerSynced instead.
	informer *genericinformers.InformerPair
	// draInformer holds the custom informer for resource.k8s.io resources,
	// kept so callers can check HasSynced without touching handler.informer.
	draInformer cache.SharedInformer
}

// Stop closes stopCh to terminate the DRA informer goroutine.
// Safe to call multiple times.
func (c *CommonResourceEventHandler) Stop() {
	select {
	case <-c.stopCh:
		// already closed
	default:
		close(c.stopCh)
	}
}

func NewCommonResourceEventHandler(
	gvr schema.GroupVersionResource,
	listenerManager *listenerManager,
	layer messagelayer.MessageLayer) *CommonResourceEventHandler {

	handler := &CommonResourceEventHandler{
		listenerManager: listenerManager,
		events:          make(chan watch.Event, 100),
		messageLayer:    layer,
		gvr:             gvr,
		stopCh:          make(chan struct{}),
	}

	klog.Infof("[dynamiccontroller/metaserver/resourceEventHandler] init handler for GVR=%v", gvr)

	if gvr.Group == "resource.k8s.io" {
		// ============================================================
		// DRA path: resource.k8s.io — custom ListWatch with a
		// standalone SharedInformer instead of the shared factory,
		// because DRA resources require SendInitialEvents on Watch.
		// ============================================================
		klog.Infof("[DRA-PATCH] using custom ListWatch for GVR=%v", gvr)

		dynClient := client.GetDynamicClient()

		lw := &cache.ListWatch{
			// (AllowWatchBookmarks, SendInitialEvents).
			// ResourceVersionMatch=NotOlderThan is valid here to get
			// a consistent list snapshot.
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				options.ResourceVersionMatch = metav1.ResourceVersionMatchNotOlderThan
				klog.Infof("[DRA-PATCH] LIST GVR=%v options=%+v", gvr, options)
				return dynClient.Resource(gvr).
					Namespace(metav1.NamespaceAll).
					List(context.TODO(), options)
			},

			// SendInitialEvents belongs here (Watch), not in ListFunc.
			// ResourceVersion must be "" (not "0") when SendInitialEvents
			// is true; "0" means "serve from cache at any version" which
			// conflicts with the streaming-list semantics of SendInitialEvents.
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				sendInitialEvents := true
				options.SendInitialEvents = &sendInitialEvents
				options.AllowWatchBookmarks = true
				options.ResourceVersionMatch = metav1.ResourceVersionMatchNotOlderThan
				options.ResourceVersion = "" //must be "" with SendInitialEvents

				klog.Infof("[DRA-PATCH] WATCH GVR=%v options=%+v", gvr, options)
				return dynClient.Resource(gvr).
					Namespace(metav1.NamespaceAll).
					Watch(context.TODO(), options)
			},
		}

		informer := cache.NewSharedInformer(
			lw,
			&unstructured.Unstructured{},
			0, // resyncPeriod=0 disables periodic re-list; events come via Watch
		)

		_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				handler.objToEvent(watch.Added, obj)
			},
			UpdateFunc: func(oldObj, obj interface{}) {
				handler.objToEvent(watch.Modified, obj)
			},
			DeleteFunc: func(obj interface{}) {
				handler.objToEvent(watch.Deleted, obj)
			},
		})
		if err != nil {
			klog.Exitf("[DRA-PATCH] AddEventHandler failed for GVR=%v err=%v", gvr, err)
		}

		// save reference so AddListener can check HasSynced
		// without touching handler.informer (which is nil on this path).
		handler.draInformer = informer

		go informer.Run(handler.stopCh)

		// Wait for the cache to sync before declaring the handler ready.
		// This prevents AddListener from listing an empty cache immediately
		// after construction.
		if !cache.WaitForCacheSync(handler.stopCh, informer.HasSynced) {
			klog.Warningf("[DRA-PATCH] cache sync timed out / stopCh closed for GVR=%v", gvr)
		} else {
			klog.Infof("[DRA-PATCH] informer cache synced for GVR=%v", gvr)
		}

		klog.Infof("[DRA-PATCH] informer started for GVR=%v", gvr)

	} else {
		// ============================================================
		// Original path: all non-DRA resources use the shared
		// informer factory managed by GetInformersManager().
		// ============================================================
		klog.Infof("[dynamiccontroller] using default informer for GVR=%v", gvr)

		informerPair, err := genericinformers.GetInformersManager().GetInformerPair(gvr)
		if err != nil {
			klog.Exitf("get informer for %s err: %v", gvr.String(), err)
		}

		_, err = informerPair.Informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				handler.objToEvent(watch.Added, obj)
			},
			UpdateFunc: func(oldObj, obj interface{}) {
				handler.objToEvent(watch.Modified, obj)
			},
			DeleteFunc: func(obj interface{}) {
				handler.objToEvent(watch.Deleted, obj)
			},
		})
		if err != nil {
			klog.Exitf("add event handler err: %v", err)
		}

		handler.informer = informerPair
	}

	klog.Infof("[dynamiccontroller/metaserver/resourceEventHandler] handler(%v) ready", gvr)

	go handler.dispatchEvents()
	return handler
}

func (c *CommonResourceEventHandler) objToEvent(t watch.EventType, obj interface{}) {
	eventObj, ok := obj.(runtime.Object)
	if !ok {
		klog.Warningf("Unknown type: %T, ignore", obj)
		return
	}
	// All obj from client has been removed the information of apiversion/kind called MetaType,
	// which is fatal to decode the obj as unstructured.Unstructure or unstructured.UnstructureList at edge.
	err := util.SetMetaType(eventObj)
	if err != nil {
		klog.Warningf("Failed to set metatype :%v", err)
	}
	c.events <- watch.Event{Type: t, Object: eventObj}
}

// AddListener sends all current objects to the new listener then registers it
// for future events.
func (c *CommonResourceEventHandler) AddListener(s *SelectorListener) error {
	if c.informer == nil {
		// DRA path — no Lister available from the shared factory.
		// The custom informer's cache is not directly exposed as a Lister here,
		// so we skip the initial sendAllObjects and rely on SendInitialEvents
		// from the WatchFunc to deliver the current state to the edge node.
		// Listeners will receive Add events for all existing objects as the
		// informer replays them through objToEvent upon registration.
		c.listenerManager.AddListener(s)
		return nil
	}

	// Original path: list current objects from the shared informer's cache
	// and send them all to the new listener before registering it.
	ret, err := c.informer.Lister.List(s.selector.Label)
	if err != nil {
		return fmt.Errorf("failed to list: %v", err)
	}
	s.sendAllObjects(ret, c)

	c.listenerManager.AddListener(s)
	return nil
}

func (c *CommonResourceEventHandler) DeleteListener(s *SelectorListener) {
	c.listenerManager.DeleteListener(s)
}

func (c *CommonResourceEventHandler) dispatchEvents() {
	for event := range c.events {
		listeners := c.listenerManager.GetListenersForGVR(c.gvr)
		for _, listener := range listeners {
			listener.sendObj(event, c.messageLayer)
		}
	}
	klog.Warningf("[metaserver/resourceEventHandler] handler(%v) stopped!", c.gvr.String())
}
