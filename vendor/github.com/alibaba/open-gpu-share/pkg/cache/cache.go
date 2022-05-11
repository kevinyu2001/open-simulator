package cache

import (
	"errors"
	"log"
	"sync"

	"github.com/alibaba/open-gpu-share/pkg/utils"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

type SchedulerCache struct {
	nodes     map[string]*GpuNodeInfo // key: node name.
	getter    NodePodGetter           // nodeLister and podLister
	knownPods map[types.UID]*v1.Pod   // pods are added when certain annotation is added and removed when complete and deleted
	nLock     *sync.RWMutex
}

type NodePodGetter interface {
	NodeGet(name string) (*v1.Node, error)
	PodGet(name string, namespace string) (*v1.Pod, error)
}

// a GPU group include 2 GPUs, which is the granularity to allocate GPU
type GpuGroupId int

func NewSchedulerCache(getter NodePodGetter) *SchedulerCache {
	return &SchedulerCache{
		nodes:     make(map[string]*GpuNodeInfo),
		getter:    getter,
		knownPods: make(map[types.UID]*v1.Pod),
		nLock:     new(sync.RWMutex),
	}
}

func (cache *SchedulerCache) GetGpuNodeinfos() []*GpuNodeInfo {
	nodes := []*GpuNodeInfo{}
	for _, n := range cache.nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

// BuildCache build cache when initializing
//func (cache *SchedulerCache) BuildCache() error {
//	if podList, _ := cache.podLister.List(labels.Everything()); err != nil {
//		return nil
//	} else {
//		return cache.BuildCacheFromPodList(podList)
//	}
//}

func (cache *SchedulerCache) BuildCacheFromPodList(podList []*v1.Pod) error {
	//log.Println("debug: begin to build scheduler cache")
	for _, pod := range podList {
		if utils.GetGpuMemoryFromPodAnnotation(pod) <= int64(0) {
			continue
		}

		if len(pod.Spec.NodeName) == 0 {
			continue
		}

		if err := cache.AddOrUpdatePod(pod); err != nil {
			return err
		}
	}
	return nil
}

func (cache *SchedulerCache) GetPod(name, namespace string) (*v1.Pod, error) {
	return cache.getter.PodGet(name, namespace)
}

// KnownPod Get known pod from the pod UID
func (cache *SchedulerCache) KnownPod(podUID types.UID) bool {
	cache.nLock.RLock()
	defer cache.nLock.RUnlock()

	_, found := cache.knownPods[podUID]
	return found
}

func (cache *SchedulerCache) AddOrUpdatePod(pod *v1.Pod) error {
	//log.Printf("debug: Add or update pod info: %v", pod)
	//log.Printf("debug: Node %v", cache.nodes)
	if len(pod.Spec.NodeName) == 0 {
		//log.Printf("debug: pod %s in ns %s is not assigned to any node, skip", pod.Name, pod.Namespace)
		return nil
	}

	n, err := cache.GetGpuNodeInfo(pod.Spec.NodeName)
	if err != nil {
		return err
	}
	podCopy := pod.DeepCopy()
	if n.addOrUpdatePod(podCopy) {
		// put it into known pod
		cache.rememberPod(podCopy)
	} else {
		//log.Printf("debug: pod %s in ns %s's gpu id is %d, it's illegal, skip", pod.Name, pod.Namespace, utils.GetGpuIdFromAnnotation(pod))
	}

	return nil
}

// The lock is in cacheNode
func (cache *SchedulerCache) RemovePod(pod *v1.Pod) {
	//log.Printf("debug: Remove pod info: %v", pod)
	//log.Printf("debug: Node %v", cache.nodes)
	n, err := cache.GetGpuNodeInfo(pod.Spec.NodeName)
	if err == nil {
		n.removePod(pod)
	} else {
		//log.Printf("debug: Failed to get node %s due to %v", pod.Spec.NodeName, err)
	}

	cache.forgetPod(pod.UID)
}

// Get or build nodeInfo if it doesn't exist
func (cache *SchedulerCache) GetGpuNodeInfo(name string) (*GpuNodeInfo, error) {
	node, err := cache.getter.NodeGet(name)
	if err != nil {
		return nil, err
	}

	cache.nLock.Lock()
	defer cache.nLock.Unlock()
	n, ok := cache.nodes[name]

	if !ok {
		n = NewGpuNodeInfo(node)
		cache.nodes[name] = n
	} else {
		// if the existing node turn from non gpushare to gpushare
		// if (utils.GetTotalGpuMemory(n.node) <= 0 && utils.GetTotalGpuMemory(node) > 0) ||
		// 	(utils.GetGpuCountInNode(n.node) <= 0 && utils.GetGpuCountInNode(node) > 0) ||
		// 	// if the existing node turn from gpushare to non gpushare
		// 	(utils.GetTotalGpuMemory(n.node) > 0 && utils.GetTotalGpuMemory(node) <= 0) ||
		// 	(utils.GetGpuCountInNode(n.node) > 0 && utils.GetGpuCountInNode(node) <= 0) {
		if len(cache.nodes[name].devs) == 0 ||
			utils.GetTotalGpuMemory(n.node) <= 0 ||
			utils.GetGpuCountInNode(n.node) <= 0 {
			//log.Printf("info: GetGpuNodeInfo() need update node %s", name)

			// fix the scenario that the number of devices changes from 0 to an positive number
			cache.nodes[name].Reset(node)
			//log.Printf("info: node: %s, labels from cache after been updated: %v", n.node.Name, n.node.Labels)
		} else {
			//log.Printf("info: GetGpuNodeInfo() uses the existing nodeInfo for %s", name)
		}
		//log.Printf("debug: node %s with devices %v", name, n.devs)
	}
	return n, nil
}

func (cache *SchedulerCache) forgetPod(uid types.UID) {
	cache.nLock.Lock()
	defer cache.nLock.Unlock()
	delete(cache.knownPods, uid)
}

func (cache *SchedulerCache) rememberPod(pod *v1.Pod) {
	cache.nLock.Lock()
	defer cache.nLock.Unlock()
	cache.knownPods[pod.UID] = pod
}

func (cache *SchedulerCache) ExportGpuNodeInfoAsStr(nodeName string) (*GpuNodeInfoStr, error) {
	if gpuNodeInfo, err := cache.GetGpuNodeInfo(nodeName); err != nil {
		return gpuNodeInfo.ExportGpuNodeInfoAsStr(), nil
	} else {
		return nil, err
	}
}

func (cache *SchedulerCache) GpuNodeAdd(node *v1.Node) error {
	if nodeInfo, ok := cache.nodes[node.Name]; ok {
		nodeInfo.rwmu.Lock()
		defer nodeInfo.rwmu.Unlock()
		nodeInfo.gpuCount = utils.GetGpuCountInNode(node)
		nodeInfo.gpuTotalMemory = utils.GetTotalGpuMemory(node)
		nodeInfo.node = node

		addGpuNum := nodeInfo.gpuCount - len(nodeInfo.devs)
		indexPointer := 0
		for i := 0; i < addGpuNum; indexPointer++ {
			if dev, ok := nodeInfo.devs[indexPointer]; !ok || dev == nil {
				nodeInfo.devs[indexPointer] = newDeviceInfo(indexPointer,
					nodeInfo.gpuTotalMemory/int64(nodeInfo.gpuCount),
					utils.GetGpuModel(node))
				i++
			}
		}
	}
	return nil
}

// modify the node, increase or decrease the number of GPUs allocated
func (cache *SchedulerCache) GpuNodeRemove(node *v1.Node, removeGpuGroup []GpuGroupId) error {
	if !cache.ValidateGpuRemove(node, removeGpuGroup) {
		return errors.New("remove illegally")
	}

	if nodeInfo, ok := cache.nodes[node.Name]; ok {
		nodeInfo.rwmu.Lock()
		defer nodeInfo.rwmu.Unlock()
		nodeInfo.gpuCount = utils.GetGpuCountInNode(node)
		nodeInfo.gpuTotalMemory = utils.GetTotalGpuMemory(node)
		nodeInfo.node = node

		for index := range nodeInfo.devs {
			if inRemoveNodes(index, removeGpuGroup) {
				delete(nodeInfo.devs, index)
			}
		}
	}
	return nil
}

func inRemoveNodes(index int, removeGpuGroup []GpuGroupId) bool {
	for _, groupId := range removeGpuGroup {
		if index == 2*int(groupId) || index == 2*int(groupId)+1 {
			return true
		}
	}
	return false
}

// check if there are pods utilizing the GPUs which will be removed
func (cache *SchedulerCache) ValidateGpuRemove(node *v1.Node, removeGpuGroup []GpuGroupId) bool {
	var nodeInfo *GpuNodeInfo
	nodeInfo, ok := cache.nodes[node.Name]
	if !ok {
		// no such node in cache, which means that
		// no pod has ever been scheduled on this node
		return true
	}
	for _, groupId := range removeGpuGroup {
		if deviceInfo, ok := nodeInfo.devs[2*int(groupId)]; !ok {
			log.Printf("no GPU with id = %v in the node", 2*int(groupId))
			return false
		} else {
			if len(deviceInfo.podMap) != 0 || deviceInfo.GetUsedGpuMemory() != 0 {
				return false
			}
		}
		if deviceInfo, ok := nodeInfo.devs[2*int(groupId)+1]; !ok {
			log.Printf("no GPU with id = %v in the node", 2*int(groupId)+1)
			return false
		} else {
			if len(deviceInfo.podMap) != 0 || deviceInfo.GetUsedGpuMemory() != 0 {
				return false
			}
		}
	}
	return true
}
