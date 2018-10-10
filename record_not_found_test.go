package gorm_test

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/leocomelli/gorm"
)

func TestRecordNotFound(t *testing.T) {
	assert := assert.New(t)

	assert.True(gorm.IsRecordNotFoundError(gorm.ErrRecordNotFound))
	assert.False(gorm.IsRecordNotFoundError(gorm.ErrCantStartTransaction))
	assert.False(gorm.IsRecordNotFoundError(errors.New("error test")))
}
