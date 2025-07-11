package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetNewLevels(t *testing.T) {
	compactionPlan := &CompactionPlan{
		baseSegments: []SegmentMetadata{{
			id:    1,
			level: 0,
		}},
		overlappingSegments: []SegmentMetadata{
			{id: 2, level: 1},
		},
	}

	levels := [][]SegmentMetadata{
		{{id: 1, level: 0}},
		{{
			id:    2,
			level: 1,
		},
			{id: 3,
				level: 1,
			},
		},
	}

	segmentMetadatList := []SegmentMetadata{
		{id: 4, level: 1}, {id: 5, level: 1},
	}

	newLevels := getNewLevels(compactionPlan, levels, segmentMetadatList)
	assert.Equal(t, [][]SegmentMetadata{
		{},
		{SegmentMetadata{
			id: 3, level: 1,
		}, SegmentMetadata{
			id: 4, level: 1,
		}, SegmentMetadata{
			id: 5, level: 1,
		}},
	}, newLevels)
}

func TestDeleteOldSegmentsAndIndexes(t *testing.T) {

}
