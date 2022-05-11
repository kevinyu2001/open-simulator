package plugin

import (
	"context"
	"errors"
	"fmt"
	"k8s.io/klog/v2"
	"log"
	"math"
	"sync"

	gpusharecache "github.com/alibaba/open-gpu-share/pkg/cache"
	gpushareutils "github.com/alibaba/open-gpu-share/pkg/utils"
	"github.com/alibaba/open-simulator/pkg/algo"
	simontype "github.com/alibaba/open-simulator/pkg/type"
	"github.com/pquerna/ffjson/ffjson"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	externalclientset "k8s.io/client-go/kubernetes"
	resourcehelper "k8s.io/kubectl/pkg/util/resource"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// GpuSharePlugin is a plugin for scheduling framework
type GpuSharePlugin struct {
	sync.RWMutex
	fakeclient          externalclientset.Interface
	cache               *gpusharecache.SchedulerCache
	podToUpdateCacheMap map[string]*corev1.Pod // key: getPodMapKey(): return pod.Namespace+pod.Name
	iswConfig           IswConfigMap
}

type NodeConfig struct {
	node      *corev1.Node
	gpuNumber int
}

type IswConfigMap struct {
	Init      bool
	IswToNode map[string][]string
	NodeToIsw map[string]string
}

// Just to check whether the implemented struct fits the interface
var _ framework.FilterPlugin = &GpuSharePlugin{}
var _ framework.ScorePlugin = &GpuSharePlugin{}
var _ framework.ReservePlugin = &GpuSharePlugin{}
var _ framework.BindPlugin = &GpuSharePlugin{}

func NewGpuSharePlugin(fakeclient externalclientset.Interface, configuration runtime.Object, f framework.Handle) (framework.Plugin, error) {
	gpuSharePlugin := &GpuSharePlugin{fakeclient: fakeclient, podToUpdateCacheMap: make(map[string]*corev1.Pod)}
	gpuSharePlugin.InitSchedulerCache()
	gpuSharePlugin.iswConfig.Init = false
	return gpuSharePlugin, nil
}

// Name returns name of the plugin. It is used in logs, etc.
func (plugin *GpuSharePlugin) Name() string {
	return simontype.OpenGpuSharePluginName
}

// Filter Plugin
// Filter filters out non-allocatable nodes
func (plugin *GpuSharePlugin) Filter(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	// check if the pod requires GPU resources
	podGpuMem := gpushareutils.GetGpuMemoryFromPodAnnotation(pod)
	if podGpuMem <= 0 {
		// the node is schedulable if pod does not require GPU resources
		//klog.Infof("[Filter] Pod: %v/%v, podGpuMem <= 0: %v", pod.GetNamespace(), pod.GetName(), podGpuMem)
		return framework.NewStatus(framework.Success)
	}
	//klog.Infof("[Filter] Pod: %v/%v, podGpuMem: %v", pod.GetNamespace(), pod.GetName(), podGpuMem)

	// check if the node have GPU resources
	node := nodeInfo.Node()
	nodeGpuMem := gpushareutils.GetTotalGpuMemory(node)
	if nodeGpuMem < podGpuMem {
		//klog.Infof("[Filter] Unschedulable, Node: %v, nodeGpuMem: %v", node.GetName(), nodeGpuMem)
		return framework.NewStatus(framework.Unschedulable, "Node:"+nodeInfo.Node().Name)
	}
	//klog.Infof("[Filter] Schedulable, Node: %v, nodeGpuMem: %v", node.GetName(), nodeGpuMem)

	// check if any of the GPU has such resources
	gpuNodeInfo, err := plugin.cache.GetGpuNodeInfo(node.Name)
	if err != nil {
		return framework.NewStatus(framework.Unschedulable, "Node:"+nodeInfo.Node().Name)
	}
	_, found := gpuNodeInfo.AllocateGpuId(pod)
	if !found {
		return framework.NewStatus(framework.Unschedulable, "Node:"+nodeInfo.Node().Name)
	}

	return framework.NewStatus(framework.Success)
}

// Score Plugin
// Score invoked at the score extension point.
func (plugin *GpuSharePlugin) Score(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeName string) (int64, *framework.Status) {
	log.Printf("score %s for %s", nodeName, pod.Name)
	podReq, _ := resourcehelper.PodRequestsAndLimits(pod)
	if len(podReq) == 0 {
		return framework.MaxNodeScore, framework.NewStatus(framework.Success)
	}

	node, err := plugin.fakeclient.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})
	if err != nil {
		return int64(framework.MinNodeScore), framework.NewStatus(framework.Error, fmt.Sprintf("failed to get node %s: %s\n", nodeName, err.Error()))
	}

	res := float64(0)
	for resourceName := range node.Status.Allocatable {
		podAllocatedRes := podReq[resourceName]
		nodeAvailableRes := node.Status.Allocatable[resourceName]
		nodeAvailableRes.Sub(podAllocatedRes)
		share := algo.Share(podAllocatedRes.AsApproximateFloat64(), nodeAvailableRes.AsApproximateFloat64())
		if share > res {
			res = share
		}
	}

	score := int64(float64(framework.MaxNodeScore-framework.MinNodeScore) * res)
	//klog.Infof("[Score] Pod: %v at Node: %v => Score: %d", pod.Name, nodeName, score)
	return score, framework.NewStatus(framework.Success)
}

// ScoreExtensions of the Score plugin.
func (plugin *GpuSharePlugin) ScoreExtensions() framework.ScoreExtensions {
	return plugin // if there is no NormalizeScore, return nil.
}

// NormalizeScore invoked after scoring all nodes.
func (plugin *GpuSharePlugin) NormalizeScore(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, scores framework.NodeScoreList) *framework.Status {
	// Find highest and lowest scores.
	var highest int64 = -math.MaxInt64
	var lowest int64 = math.MaxInt64
	for _, nodeScore := range scores {
		if nodeScore.Score > highest {
			highest = nodeScore.Score
		}
		if nodeScore.Score < lowest {
			lowest = nodeScore.Score
		}
	}

	// Transform the highest to lowest score range to fit the framework's min to max node score range.
	oldRange := highest - lowest
	newRange := framework.MaxNodeScore - framework.MinNodeScore
	for i, nodeScore := range scores {
		if oldRange == 0 {
			scores[i].Score = framework.MinNodeScore
		} else {
			scores[i].Score = ((nodeScore.Score - lowest) * newRange / oldRange) + framework.MinNodeScore
		}
	}

	return framework.NewStatus(framework.Success)
}

// Reserve Plugin
// Reserve updates the GPU resource of the given node, according to the pod's request.
func (plugin *GpuSharePlugin) Reserve(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeName string) *framework.Status {
	if gpushareutils.GetGpuMemoryFromPodAnnotation(pod) <= 0 {
		return framework.NewStatus(framework.Success) // non-GPU pods are skipped
	}
	plugin.Lock()
	defer plugin.Unlock()

	// get PodCopy but NOT update it
	podCopy, err := plugin.MakePodCopyReadyForBindUpdate(pod, nodeName)
	if err != nil {
		klog.Errorf("The node %s can't place the pod %s in ns %s,and the pod spec is %v. err: %s", pod.Spec.NodeName, pod.Name, pod.Namespace, pod, err)
		return framework.NewStatus(framework.Error, err.Error())
	}
	plugin.podToUpdateCacheMap[getPodMapKey(pod)] = podCopy

	// get node from fakeclient and update Node
	node, _ := plugin.fakeclient.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})
	if err := plugin.cache.AddOrUpdatePod(podCopy); err != nil { // requires pod.Spec.NodeName specified
		return framework.NewStatus(framework.Error, err.Error())
	}
	nodeGpuInfo, err := plugin.ExportGpuNodeInfoAsGpuNodeInfoStr(nodeName)
	if err != nil {
		return framework.NewStatus(framework.Error, err.Error())
	}
	if data, err := ffjson.Marshal(nodeGpuInfo); err != nil {
		return framework.NewStatus(framework.Error, err.Error())
	} else {
		metav1.SetMetaDataAnnotation(&node.ObjectMeta, simontype.AnnoNodeGpuShare, string(data))
	}

	infoValue := int64(nodeGpuInfo.GpuAllocatable)
	allocValue := node.Status.Allocatable[gpushareutils.CountName]
	if allocValue.Value() != infoValue {
		//klog.Infof("node %s: number of full GPU allocatable updated: %s -> %d", node.Name, allocValue.String(), infoValue)
		allocValue.Set(infoValue)
	}

	if _, err := plugin.fakeclient.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{}); err != nil {
		return framework.NewStatus(framework.Error, err.Error())
	}
	return framework.NewStatus(framework.Success)
}

// Unreserve undoes the GPU resource updated in Reserve function.
func (plugin *GpuSharePlugin) Unreserve(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeName string) {
	plugin.Lock()
	defer plugin.Unlock()
	node, _ := plugin.fakeclient.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})

	if podCopy, ok := plugin.podToUpdateCacheMap[getPodMapKey(pod)]; !ok {
		//klog.Errorf("Cannot find pod to update in cache")
		return
	} else {
		plugin.cache.RemovePod(podCopy)
	}
	nodeGpuInfo, _ := plugin.ExportGpuNodeInfoAsGpuNodeInfoStr(nodeName)
	if data, err := ffjson.Marshal(nodeGpuInfo); err != nil {
		klog.Errorf("Marshal nodeGpuInfo failed")
		return
	} else {
		metav1.SetMetaDataAnnotation(&node.ObjectMeta, simontype.AnnoNodeGpuShare, string(data))
	}

	infoValue := int64(nodeGpuInfo.GpuAllocatable)
	allocValue := node.Status.Allocatable[gpushareutils.CountName]
	if allocValue.Value() != infoValue {
		//klog.Infof("node %s: number of full GPU allocatable updated: %s -> %d", node.Name, allocValue.String(), infoValue)
		allocValue.Set(infoValue)
	}

	if _, err := plugin.fakeclient.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{}); err != nil {
		klog.Errorf("Failed to Update node")
		return
	}
}

// Bind Plugin
// Bind updates the GPU resources of the pod.
func (plugin *GpuSharePlugin) Bind(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeName string) *framework.Status {
	if gpushareutils.GetGpuMemoryFromPodAnnotation(pod) <= 0 {
		return framework.NewStatus(framework.Skip) // non-GPU pods are skipped
	}
	plugin.Lock()
	defer plugin.Unlock()

	podCopy, ok := plugin.podToUpdateCacheMap[getPodMapKey(pod)]
	if !ok {
		klog.Errorf("No podToUpdate found, which should not happen since it should have failed in ReservePlugin")
		return framework.NewStatus(framework.Error, "No podToUpdate found")
	}
	_, err := plugin.fakeclient.CoreV1().Pods(podCopy.Namespace).Update(context.TODO(), podCopy, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("fake update error %v", err)
		return framework.NewStatus(framework.Error, fmt.Sprintf("Unable to add new pod: %v", err))
	}
	delete(plugin.podToUpdateCacheMap, getPodMapKey(pod)) // avoid memory leakage

	node, err := plugin.fakeclient.CoreV1().Nodes().Get(context.Background(), podCopy.Spec.NodeName, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("fake get error %v", err)
	}
	if err = plugin.IswReallocate(node, pod); err != nil {
		klog.Errorf("ISW reallocate error %v", err)
	}

	//klog.Infof("Allocate() ---- pod %s in ns %s is allocated to node %s ----", podCopy.Name, podCopy.Namespace, podCopy.Spec.NodeName)
	return nil
}

// Util Functions

func (plugin *GpuSharePlugin) ExportGpuNodeInfoAsGpuNodeInfoStr(nodeName string) (*gpusharecache.GpuNodeInfoStr, error) {
	if gpuNodeInfo, err := plugin.cache.GetGpuNodeInfo(nodeName); err != nil {
		return nil, err
	} else {
		nodeGpuInfo := gpuNodeInfo.ExportGpuNodeInfoAsStr()
		return nodeGpuInfo, nil
	}
}

func (plugin *GpuSharePlugin) NodeGet(name string) (*corev1.Node, error) {
	return plugin.fakeclient.CoreV1().Nodes().Get(context.Background(), name, metav1.GetOptions{})
}

func (plugin *GpuSharePlugin) PodGet(name string, namespace string) (*corev1.Pod, error) {
	return plugin.fakeclient.CoreV1().Pods(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

func (plugin *GpuSharePlugin) InitSchedulerCache() {
	plugin.cache = gpusharecache.NewSchedulerCache(plugin) // here `plugin` implements the NodePodGetter interface
}

func (plugin *GpuSharePlugin) InitIswConfig() {
	if plugin.iswConfig.Init {
		return
	}
	plugin.iswConfig.IswToNode = make(map[string][]string)
	plugin.iswConfig.NodeToIsw = make(map[string]string)
	nodeList, err := plugin.fakeclient.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		log.Printf("unable to list all nodes")
		return
	}
	for _, node := range nodeList.Items {
		if iswName := gpushareutils.GetIswName(&node); iswName != "N/A" {
			//log.Printf("add node %s with ISW %s to map", node.Name, iswName)
			plugin.iswConfig.NodeToIsw[node.Name] = iswName
		} else {
			//log.Printf("node %s is not an ISW node, refuse", node.Name)
		}
	}
	for nodeName, iswName := range plugin.iswConfig.NodeToIsw {
		plugin.iswConfig.IswToNode[iswName] = append(plugin.iswConfig.IswToNode[iswName], nodeName)
	}
	plugin.iswConfig.Init = true
	return
}

func (plugin *GpuSharePlugin) MakePodCopyReadyForBindUpdate(pod *corev1.Pod, nodeName string) (*corev1.Pod, error) {
	gpuNodeInfo, err := plugin.cache.GetGpuNodeInfo(nodeName)
	if err != nil {
		return nil, err
	}

	devId, found := gpuNodeInfo.AllocateGpuId(pod)
	log.Printf("%s take use of GPU %s", pod.Name, devId)
	if !found {
		err := fmt.Errorf("Cannot find a GPU to allocate pod %s at ns %s", pod.Name, pod.Namespace)
		return nil, err
	}

	podCopy := gpushareutils.GetUpdatedPodAnnotationSpec(pod, devId)
	podCopy.Spec.NodeName = nodeName
	podCopy.Status.Phase = corev1.PodRunning
	return podCopy, nil
}

func getPodMapKey(pod *corev1.Pod) string {
	return pod.Namespace + pod.Name
}

// ISW reallocate GPUs according to pods on these nodes
func (plugin *GpuSharePlugin) IswReallocate(node *corev1.Node, pod *corev1.Pod) error {
	// Step 1: find neighbors of the node
	log.Printf("ISW reallocate when binding %s to %s", pod.Name, node.Name)
	log.Printf("ISW reallocate step 1")
	plugin.InitIswConfig()
	if iswName := gpushareutils.GetIswName(node); iswName == "N/A" {
		return nil
	}
	neighborNames, err := plugin.getNeighborNames(node)
	if err != nil {
		return err
	}
	var neighborList = make([]*corev1.Node, len(neighborNames))
	for i, nodeName := range neighborNames {
		if nodeName == node.Name {
			neighborList[i] = node
			continue
		}
		neighborList[i], err = plugin.fakeclient.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})
		if err != nil {
			return err
		}
	}

	// Step 2: determine if the node should give GPUs to other nodes
	log.Printf("ISW reallocate step 2")
	var neighborScore = make([]float64, len(neighborList))
	var moveFlag = false
	for i, neighbor := range neighborList {
		if neighbor == node {
			if neighborScore[i], err = plugin.IswNodeScore(neighbor, nil, 0); err != nil {
				return err
			}
			if neighborScore[i] < simontype.CpuPerGpuThreshold {
				log.Printf("node's score is %v, ISW reallocate GPUs", neighborScore[i])
				moveFlag = true
			}
			continue
		}
		if neighborScore[i], err = plugin.IswNodeScore(neighbor, nil, 0); err != nil {
			return err
		}
	}
	if !moveFlag {
		log.Printf("node's resource is enough, no need to reallocate GPUs")
		return nil
	}

	// Step 3: determine how to reallocate GPUs
	log.Printf("ISW reallocate step 3")
	var freeGpuGroup []gpusharecache.GpuGroupId
	if freeGpuGroup, err = plugin.FreeGpuGroup(node); err != nil {
		return nil
	}
	log.Printf("free GPU group: %v", freeGpuGroup)

	// determine how many GPUs should be moved
	var freeGpu = 2 * int64(len(freeGpuGroup))
	var moveGpu int64
	if freeGpu == 0 {
		log.Printf("no free GPU in this node, cancle reallocating")
		return nil
	} else if freeGpu == 2 {
		moveGpu = freeGpu
	} else {
		for moveGpu = 2; moveGpu <= freeGpu; moveGpu += 2 {
			score, err := plugin.IswNodeScore(node, nil, -moveGpu)
			if err != nil {
				return err
			}
			log.Printf("when move %v GPUs, node's score is %v", moveGpu, score)
			if score >= simontype.CpuPerGpuThreshold {
				break
			}
		}
	}

	// determine which node receive these GPUs
	var highestScore float64 = 0
	var highestNode *corev1.Node
	var currentScore float64
	var movedScore float64
	for i, neighbor := range neighborList {
		if neighbor == node {
			currentScore = neighborScore[i]
			continue
		}
		if neighborScore[i] > highestScore {
			highestScore = neighborScore[i]
			highestNode = neighbor
		}
	}
	if movedScore, err = plugin.IswNodeScore(highestNode, nil, int64(moveGpu)); err != nil {
		return err
	}
	if movedScore < currentScore {
		log.Printf("there is no node with enough idle CPUs")
		return nil
	}
	log.Printf("determine to move %v GPUs from %v to %v", moveGpu, node.Name, highestNode.Name)

	// Step 4: update two nodes in fakeclient
	// you should set gpu-count and gpu-mem as 0
	// in ISW nodes having no GPU temporarily
	log.Printf("ISW reallocate step 4")
	gpuCount := node.Status.Capacity[gpushareutils.CountName]
	oldGpuCount := gpuCount.Value()
	gpuCount.Set(gpuCount.Value() - moveGpu)
	node.Status.Capacity[gpushareutils.CountName] = gpuCount

	gpuCount = node.Status.Allocatable[gpushareutils.CountName]
	gpuCount.Set(gpuCount.Value() - moveGpu)
	node.Status.Allocatable[gpushareutils.CountName] = gpuCount

	gpuCount = highestNode.Status.Capacity[gpushareutils.CountName]
	gpuCount.Set(gpuCount.Value() + moveGpu)
	highestNode.Status.Capacity[gpushareutils.CountName] = gpuCount

	gpuCount = highestNode.Status.Allocatable[gpushareutils.CountName]
	gpuCount.Set(gpuCount.Value() + moveGpu)
	highestNode.Status.Allocatable[gpushareutils.CountName] = gpuCount

	gpuTotalMem := node.Status.Capacity[gpushareutils.ResourceName]
	moveGpuMem := gpuTotalMem.Value() * moveGpu / oldGpuCount
	gpuTotalMem.Set(gpuTotalMem.Value() - moveGpuMem)
	node.Status.Capacity[gpushareutils.ResourceName] = gpuTotalMem

	gpuTotalMem = node.Status.Allocatable[gpushareutils.ResourceName]
	gpuTotalMem.Set(gpuTotalMem.Value() - moveGpuMem)
	node.Status.Allocatable[gpushareutils.ResourceName] = gpuTotalMem

	gpuTotalMem = highestNode.Status.Capacity[gpushareutils.ResourceName]
	gpuTotalMem.Set(gpuTotalMem.Value() + moveGpuMem)
	highestNode.Status.Capacity[gpushareutils.ResourceName] = gpuTotalMem

	gpuTotalMem = highestNode.Status.Allocatable[gpushareutils.ResourceName]
	gpuTotalMem.Set(gpuTotalMem.Value() + moveGpuMem)
	highestNode.Status.Allocatable[gpushareutils.ResourceName] = gpuTotalMem

	//log.Printf("node:\n%v\nhighest node:\n%v", node, highestNode)

	if _, err := plugin.fakeclient.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{}); err != nil {
		return err
	}
	if _, err := plugin.fakeclient.CoreV1().Nodes().Update(context.Background(), highestNode, metav1.UpdateOptions{}); err != nil {
		return err
	}

	// Step 5: update two nodes in open-gpu-share cache
	log.Printf("ISW reallocate step 5")
	removeGpuGroup := freeGpuGroup[:moveGpu/2]
	if err := plugin.cache.GpuNodeRemove(node, removeGpuGroup); err != nil {
		log.Printf("error: cannot remove GPUs in %s", node.Name)
		return err
	}
	if err := plugin.cache.GpuNodeAdd(highestNode); err != nil {
		log.Printf("error: cannot add GPUs in %s", highestNode.Name)
		return err
	}

	log.Printf("reallocate finished!")
	return nil
}

// return neighbors of a node (including itself)
func (plugin *GpuSharePlugin) getNeighborNames(node *corev1.Node) (neighborNames []string, err error) {
	iswName, ok := plugin.iswConfig.NodeToIsw[node.Name]
	if !ok {
		err := errors.New(fmt.Sprintf("node-ISW map error: there is no node named %s in the map", node.Name))
		log.Printf("getNeighbors error: %v", err)
		return nil, err
	}
	if neighborNames, ok = plugin.iswConfig.IswToNode[iswName]; !ok {
		err := errors.New(fmt.Sprintf("ISW-node map error: there is no ISW named %s in the map", iswName))
		log.Printf("getNeighbors error: %v", err)
		return nil, err
	}
	return plugin.iswConfig.IswToNode[iswName], nil
}

// make sure there is no pods running on GPUs which will be removed
func (plugin *GpuSharePlugin) CacheGpuRemoveValidate(node *corev1.Node, removeGpuGroup []gpusharecache.GpuGroupId) bool {
	return plugin.cache.ValidateGpuRemove(node, removeGpuGroup)
}

func (plugin *GpuSharePlugin) IswNodeScore(node *corev1.Node, pod *corev1.Pod, extraGpu int64) (float64, error) {
	var nodeInfo *gpusharecache.GpuNodeInfo
	var freeGpu = extraGpu
	var freeCpu int64
	log.Printf("score the node %s with %v GPUs, extraGpu = %v", node.Name, gpushareutils.GetGpuCountInNode(node), extraGpu)
	nodeInfo, err := plugin.cache.GetGpuNodeInfo(node.Name)
	if err != nil {
		log.Printf("error when get GpuNodeInfo from cache")
		return 0, err
	}
	for _, gpu := range nodeInfo.GetDevs() {
		if gpu.GetUsedGpuMemory() == 0 {
			freeGpu++
		}
	}
	freeCpu = nodeInfo.GetFreeCpu()
	if pod != nil {
		podReq, _ := resourcehelper.PodRequestsAndLimits(pod)
		freeCpu -= podReq.Cpu().Value()
	}
	if freeGpu == 0 {
		log.Printf("Score: %v, free CPU: %v, free GPU: %v", float64(freeCpu)*simontype.CpuNodeScoreScale, freeCpu, freeGpu)
		return float64(freeCpu) * simontype.CpuNodeScoreScale, nil
	}
	log.Printf("Score: %v, free CPU: %v, free GPU: %v", float64(freeCpu)/float64(freeGpu), freeCpu, freeGpu)
	return float64(freeCpu) / float64(freeGpu), nil
}

func (plugin *GpuSharePlugin) FreeGpuGroup(node *corev1.Node) (freeGroup []gpusharecache.GpuGroupId, err error) {
	nodeInfo, err := plugin.cache.GetGpuNodeInfo(node.Name)
	if err != nil {
		log.Printf("error when get GpuNodeInfo from cache")
		return nil, err
	}
	devices := nodeInfo.GetDevs()
	existIndex := 0
	for index := 0; existIndex < len(devices)/2; index += 2 {
		device0, ok0 := devices[index]
		device1, ok1 := devices[index+1]
		if ok0 && ok1 {
			if device0.GetUsedGpuMemory() == 0 && device1.GetUsedGpuMemory() == 0 {
				freeGroup = append(freeGroup, gpusharecache.GpuGroupId(index/2))
			}
			existIndex++
		}
	}
	return freeGroup, nil
}
