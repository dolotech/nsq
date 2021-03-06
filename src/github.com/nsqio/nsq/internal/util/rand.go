package util

import (
	"math/rand"
)

// 产生一组不重复的随机数
func UniqRands(l int, n int) []int {
	set := make(map[int]struct{})
	nums := make([]int, 0, l)
	for {
		num := rand.Intn(n)
		if _, ok := set[num]; !ok {
			set[num] = struct{}{}
			nums = append(nums, num)
		}
		if len(nums) == l {
			goto exit
		}
	}
exit:
	return nums
}
