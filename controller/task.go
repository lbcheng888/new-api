package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/samber/lo"
	"io"
	"net/http"
	"one-api/common"
	"one-api/constant"
	"one-api/dto"
	"one-api/model"
	"one-api/relay"
	"sort"
	"strconv"
	"sync"
	"time"
)

func UpdateTaskBulk() {
	//revocer
	//imageModel := "midjourney"
	for {
		time.Sleep(time.Duration(15) * time.Second)
		ctx := context.TODO()
		allTasks := model.GetAllUnFinishSyncTasks(500)
		platformTask := make(map[constant.TaskPlatform][]*model.Task)
		for _, t := range allTasks {
			platformTask[t.Platform] = append(platformTask[t.Platform], t)
		}
		for platform, tasks := range platformTask {
			if len(tasks) == 0 {
				continue
			}
			taskChannelM := make(map[int][]string)
			taskM := make(map[string]*model.Task)
			nullTaskIds := make([]int64, 0)
			for _, task := range tasks {
				if task.TaskID == "" {
					// 统计失败的未完成任务
					nullTaskIds = append(nullTaskIds, task.ID)
					continue
				}
				taskM[task.TaskID] = task
				taskChannelM[task.ChannelId] = append(taskChannelM[task.ChannelId], task.TaskID)
			}
			if len(nullTaskIds) > 0 {
				err := model.TaskBulkUpdateByID(nullTaskIds, map[string]any{
					"status":   "FAILURE",
					"progress": "100%",
				})
				if err != nil {
					common.LogError(ctx, fmt.Sprintf("Fix null task_id task error: %v", err))
				} else {
					common.LogInfo(ctx, fmt.Sprintf("Fix null task_id task success: %v", nullTaskIds))
				}
			}
			if len(taskChannelM) == 0 {
				continue
			}
			UpdateTaskByPlatform(ctx, platform, taskChannelM, taskM)
		}
	}
}

func UpdateTaskByPlatform(ctx context.Context, platform constant.TaskPlatform, taskChannelM map[int][]string, taskM map[string]*model.Task) {
	var wg sync.WaitGroup
	switch platform {
	case constant.TaskPlatformMidjourney:
		// TODO: Implement concurrent update for Midjourney if needed
		// _ = UpdateMidjourneyTaskAll(context.Background(), tasks)
		common.SysLog("Midjourney task update not implemented for concurrency yet")
	case constant.TaskPlatformSuno:
		for channelId, taskIds := range taskChannelM {
			wg.Add(1)
			go func(cid int, tids []string) {
				defer wg.Done()
				err := updateSunoTaskAll(ctx, cid, tids, taskM)
				if err != nil {
					common.LogError(ctx, fmt.Sprintf("渠道 #%d 更新Suno异步任务失败: %s", cid, err.Error()))
				}
			}(channelId, taskIds)
		}
	default:
		common.SysLog(fmt.Sprintf("未知平台: %s", platform))
	}
	wg.Wait() // Wait for all channel updates to complete
}

// UpdateSunoTaskAll is deprecated, use UpdateTaskByPlatform with goroutines instead.
// func UpdateSunoTaskAll(ctx context.Context, taskChannelM map[int][]string, taskM map[string]*model.Task) error {
// 	for channelId, taskIds := range taskChannelM {
// 		err := updateSunoTaskAll(ctx, channelId, taskIds, taskM)
// 		if err != nil {
// 			common.LogError(ctx, fmt.Sprintf("渠道 #%d 更新异步任务失败: %s", channelId, err.Error()))
// 		}
// 	}
// 	return nil
// }
// Deprecated: Use UpdateTaskByPlatform with goroutines instead.
// func UpdateSunoTaskAll(ctx context.Context, taskChannelM map[int][]string, taskM map[string]*model.Task) error {
// 	for channelId, taskIds := range taskChannelM {
// 		err := updateSunoTaskAll(ctx, channelId, taskIds, taskM)
// 		if err != nil {
// 			common.LogError(ctx, fmt.Sprintf("渠道 #%d 更新异步任务失败: %s", channelId, err.Error()))
// 		}
// 	}
// 	return nil
// }

func updateSunoTaskAll(ctx context.Context, channelId int, taskIds []string, taskM map[string]*model.Task) error {
	common.LogInfo(ctx, fmt.Sprintf("channel #%d has %d tasks not finished", channelId, len(taskIds)))
	if len(taskIds) == 0 {
		return nil
	}
	channel, err := model.CacheGetChannel(channelId)
	if err != nil {
		common.SysLog(fmt.Sprintf("CacheGetChannel: %v", err))
		// 尝试批量更新这些任务为失败状态
		failedTaskIds := make([]int64, 0, len(taskIds))
		for _, taskIdStr := range taskIds {
			if task, ok := taskM[taskIdStr]; ok {
				failedTaskIds = append(failedTaskIds, task.ID)
			}
		}
		updateErr := model.TaskBulkUpdateByID(failedTaskIds, map[string]any{
			"fail_reason": fmt.Sprintf("failed to fetch channel info, channel_id: %d", channelId),
			"status":      model.TaskStatusFailure,
			"progress":    "100%",
		})
		if updateErr != nil {
			common.SysError(fmt.Sprintf("UpdateSunoTask error when channel fetch failed: %v", updateErr))
		}
		return err // 返回原始的获取渠道错误
	}

	adaptor := relay.GetTaskAdaptor(constant.TaskPlatformSuno)
	if adaptor == nil {
		return errors.New("adaptor not found")
	}
	resp, err := adaptor.FetchTask(*channel.BaseURL, channel.Key, map[string]any{
		"ids": taskIds,
	})
	if err != nil {
		common.SysError(fmt.Sprintf("Get Task Do req error: %v", err))
		// 注意：这里不直接返回，因为可能部分任务已经完成，需要继续处理下面的逻辑来更新这些任务的状态
		// 但如果连请求都发不出去，可能所有任务都无法更新，可以考虑在这里也批量更新为失败
		// return err
	}

	// 即使请求失败，也尝试处理可能的响应体（例如超时但部分数据返回）
	var responseItems dto.TaskResponse[[]dto.SunoDataResponse]
	if resp != nil {
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			common.LogError(ctx, fmt.Sprintf("Get Task status code: %d", resp.StatusCode))
			// 根据状态码决定是否继续，例如 5xx 错误可能不继续
			// return errors.New(fmt.Sprintf("Get Task status code: %d", resp.StatusCode))
		}

		responseBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			common.SysError(fmt.Sprintf("Get Task parse body error: %v", readErr))
			// return readErr
		} else {
			unmarshalErr := json.Unmarshal(responseBody, &responseItems)
			if unmarshalErr != nil {
				common.LogError(ctx, fmt.Sprintf("Get Task parse body error2: %v, body: %s", unmarshalErr, string(responseBody)))
				// return unmarshalErr
			}
			if !responseItems.IsSuccess() {
				common.SysLog(fmt.Sprintf("渠道 #%d 获取任务状态响应失败: %s", channelId, string(responseBody)))
				// 根据业务逻辑决定是否返回错误
			}
		}
	}

	// --- 优化后的批量更新逻辑 ---
	tasksToUpdate := make(map[model.TaskStatus][]map[string]any) // 按状态分组更新
	tasksToCompensate := make(map[int]int)                       // key: userID, value: total quota to compensate
	processedTaskIDs := make(map[string]bool)                    // 跟踪已处理的 task_id

	// 1. 收集所有需要更新的任务信息
	if responseItems.Data != nil {
		for _, responseItem := range responseItems.Data {
			task, ok := taskM[responseItem.TaskID]
			if !ok {
				common.LogWarn(ctx, fmt.Sprintf("Task %s not found in local map", responseItem.TaskID))
				continue
			}
			processedTaskIDs[task.TaskID] = true // 标记为已处理

			if !checkTaskNeedUpdate(task, responseItem) {
				continue
			}

			newStatus := model.TaskStatus(responseItem.Status)
			// 如果状态为空，则保持原状态（可能只是更新时间戳或数据）
			if newStatus == "" {
				newStatus = task.Status
			}

			updateData := map[string]any{
				"id":          task.ID, // 包含 ID 以便后续更新
				"submit_time": lo.If(responseItem.SubmitTime != 0, responseItem.SubmitTime).Else(task.SubmitTime),
				"start_time":  lo.If(responseItem.StartTime != 0, responseItem.StartTime).Else(task.StartTime),
				"finish_time": lo.If(responseItem.FinishTime != 0, responseItem.FinishTime).Else(task.FinishTime),
				"data":        responseItem.Data, // 直接使用新的 data
				"status":      newStatus,
			}

			if newStatus == model.TaskStatusSuccess {
				updateData["progress"] = "100%"
				updateData["fail_reason"] = "" // 清空失败原因
			} else if newStatus == model.TaskStatusFailure || responseItem.FailReason != "" {
				failReason := lo.If(responseItem.FailReason != "", responseItem.FailReason).Else("Unknown failure reason")
				common.LogInfo(ctx, task.TaskID+" 构建失败，"+failReason)
				updateData["progress"] = "100%"
				updateData["fail_reason"] = failReason
				// 记录需要补偿的额度
				if task.Quota > 0 {
					tasksToCompensate[task.UserId] += task.Quota
				}
			} else {
				// 对于 IN_PROGRESS 或其他非最终状态，也记录更新数据
				// 如果需要更新进度，可以在这里添加逻辑
				// updateData["progress"] = calculateProgress(...)
				updateData["fail_reason"] = lo.If(responseItem.FailReason != "", responseItem.FailReason).Else(task.FailReason) // 保留或更新失败原因
			}

			tasksToUpdate[newStatus] = append(tasksToUpdate[newStatus], updateData)
		}
	}

	// 处理那些请求中包含但API未返回状态的任务 (可能仍在进行中或API侧丢失)
	// 可以选择将这些任务标记为某种状态，或者暂时不处理，等待下次轮询
	// 这里暂时不处理，依赖下次轮询

	// 2. 执行数据库更新 (循环单独更新，添加计时)
	updateCount := 0
	updateStartTime := time.Now()
	common.LogInfo(ctx, fmt.Sprintf("Starting database updates for channel %d...", channelId))
	for status, updates := range tasksToUpdate { // tasksToUpdate 在此作用域内是可见的
		if len(updates) == 0 {
			continue
		}
		// common.LogInfo(ctx, fmt.Sprintf("Updating %d tasks to status %s", len(updates), status)) // 日志可能过于频繁，暂时注释

		// 循环单独更新 (性能较低，但确保正确性，后续可根据耗时决定是否优化为原生SQL)
		for _, updateData := range updates {
			taskId := updateData["id"].(int64)
			// 从 updateData 中移除 id，因为它不是 task 表的字段，而是用于 Where 条件
			updateFields := make(map[string]any)
			for k, v := range updateData {
				if k != "id" {
					updateFields[k] = v
				}
			}
			// 确保至少有字段需要更新
			if len(updateFields) > 0 {
				err := model.DB.Model(&model.Task{}).Where("id = ?", taskId).Updates(updateFields).Error
				if err != nil {
					common.SysError(fmt.Sprintf("Error updating task %d to status %s: %v", taskId, status, err))
					// 可以考虑记录失败的 ID 稍后重试
				} else {
					updateCount++
				}
			} else {
				common.LogWarn(ctx, fmt.Sprintf("Skipping update for task %d as no fields changed (status: %s)", taskId, status))
			}
		}
	}
	updateDuration := time.Since(updateStartTime)
	common.LogInfo(ctx, fmt.Sprintf("Finished updating %d tasks for channel %d in %v", updateCount, channelId, updateDuration))
	// 如果 updateDuration 很大 (例如超过几秒)，则提示这里是瓶颈，需要进一步优化 (如原生SQL)
	if updateDuration > 3*time.Second { // 阈值可调整
		common.LogWarn(ctx, fmt.Sprintf("Database update loop for channel %d took %v, consider optimizing with native SQL or other bulk methods.", channelId, updateDuration))
	}

	// 3. 处理配额补偿 (优化后)
	compensationStartTime := time.Now()
	compensationCount := 0
	if len(tasksToCompensate) > 0 {
		common.LogInfo(ctx, fmt.Sprintf("Processing quota compensation for %d users for channel %d...", len(tasksToCompensate), channelId))
		for userId, totalQuota := range tasksToCompensate {
			if totalQuota > 0 {
				// common.LogInfo(ctx, fmt.Sprintf("Compensating user %d with quota %d", userId, totalQuota)) // 日志可能过于频繁
				increaseErr := model.IncreaseUserQuota(userId, totalQuota, false) // 调用一次 IncreaseUserQuota
				if increaseErr != nil {
					common.LogError(ctx, fmt.Sprintf("fail to increase user quota for user %d: %s", userId, increaseErr.Error()))
				} else {
					compensationCount++
					logContent := fmt.Sprintf("批量处理失败任务，补偿用户 %d 额度 %s", userId, common.LogQuota(totalQuota))
					model.RecordLog(userId, model.LogTypeSystem, logContent)
					_ = model.InvalidateUserCache(userId) // 调用一次 InvalidateUserCache
				}
			}
		}
		common.LogInfo(ctx, fmt.Sprintf("Finished compensating quota for %d users for channel %d in %v", compensationCount, channelId, time.Since(compensationStartTime)))
	}

	// 如果最初的 adaptor.FetchTask 就失败了，在这里返回错误
	if err != nil {
		return fmt.Errorf("fetch task failed: %w", err)
	}

	return nil
}

func checkTaskNeedUpdate(oldTask *model.Task, newTask dto.SunoDataResponse) bool {

	if oldTask.SubmitTime != newTask.SubmitTime {
		return true
	}
	if oldTask.StartTime != newTask.StartTime {
		return true
	}
	if oldTask.FinishTime != newTask.FinishTime {
		return true
	}
	if string(oldTask.Status) != newTask.Status {
		return true
	}
	if oldTask.FailReason != newTask.FailReason {
		return true
	}
	if oldTask.FinishTime != newTask.FinishTime {
		return true
	}

	if (oldTask.Status == model.TaskStatusFailure || oldTask.Status == model.TaskStatusSuccess) && oldTask.Progress != "100%" {
		return true
	}

	oldData, _ := json.Marshal(oldTask.Data)
	newData, _ := json.Marshal(newTask.Data)

	sort.Slice(oldData, func(i, j int) bool {
		return oldData[i] < oldData[j]
	})
	sort.Slice(newData, func(i, j int) bool {
		return newData[i] < newData[j]
	})

	if string(oldData) != string(newData) {
		return true
	}
	return false
}

func GetAllTask(c *gin.Context) {
	p, _ := strconv.Atoi(c.Query("p"))
	if p < 0 {
		p = 0
	}
	startTimestamp, _ := strconv.ParseInt(c.Query("start_timestamp"), 10, 64)
	endTimestamp, _ := strconv.ParseInt(c.Query("end_timestamp"), 10, 64)
	// 解析其他查询参数
	queryParams := model.SyncTaskQueryParams{
		Platform:       constant.TaskPlatform(c.Query("platform")),
		TaskID:         c.Query("task_id"),
		Status:         c.Query("status"),
		Action:         c.Query("action"),
		StartTimestamp: startTimestamp,
		EndTimestamp:   endTimestamp,
	}

	logs := model.TaskGetAllTasks(p*common.ItemsPerPage, common.ItemsPerPage, queryParams)
	if logs == nil {
		logs = make([]*model.Task, 0)
	}

	c.JSON(200, gin.H{
		"success": true,
		"message": "",
		"data":    logs,
	})
}

func GetUserTask(c *gin.Context) {
	p, _ := strconv.Atoi(c.Query("p"))
	if p < 0 {
		p = 0
	}

	userId := c.GetInt("id")

	startTimestamp, _ := strconv.ParseInt(c.Query("start_timestamp"), 10, 64)
	endTimestamp, _ := strconv.ParseInt(c.Query("end_timestamp"), 10, 64)

	queryParams := model.SyncTaskQueryParams{
		Platform:       constant.TaskPlatform(c.Query("platform")),
		TaskID:         c.Query("task_id"),
		Status:         c.Query("status"),
		Action:         c.Query("action"),
		StartTimestamp: startTimestamp,
		EndTimestamp:   endTimestamp,
	}

	logs := model.TaskGetAllUserTask(userId, p*common.ItemsPerPage, common.ItemsPerPage, queryParams)
	if logs == nil {
		logs = make([]*model.Task, 0)
	}

	c.JSON(200, gin.H{
		"success": true,
		"message": "",
		"data":    logs,
	})
}
