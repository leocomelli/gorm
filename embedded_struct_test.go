package gorm_test

import (
	"os"
	"testing"
)

type BasePost struct {
	Id    int64
	Title string
	URL   string
}

type Author struct {
	ID    string
	Name  string
	Email string
}

type HNPost struct {
	BasePost
	Author  `gorm:"embedded_prefix:user_"` // Embedded struct
	Upvotes int32
}

type EngadgetPost struct {
	BasePost BasePost `gorm:"embedded"`
	Author   Author   `gorm:"embedded;embedded_prefix:author_"` // Embedded struct
	ImageUrl string
}

func TestPrefixColumnNameForEmbeddedStruct(t *testing.T) {
	if dialect := os.Getenv("GORM_DIALECT"); dialect == "oracle" {
		t.Skip("Skipping this because I do not spend time in the first round :)")
	}

	dialect := DB.NewScope(&EngadgetPost{}).Dialect()
	engadgetPostScope := DB.NewScope(&EngadgetPost{})
	if !dialect.HasColumn(engadgetPostScope.TableName(), "author_id") || !dialect.HasColumn(engadgetPostScope.TableName(), "author_name") || !dialect.HasColumn(engadgetPostScope.TableName(), "author_email") {
		t.Errorf("should has prefix for embedded columns")
	}

	if len(engadgetPostScope.PrimaryFields()) != 1 {
		t.Errorf("should have only one primary field with embedded struct, but got %v", len(engadgetPostScope.PrimaryFields()))
	}

	hnScope := DB.NewScope(&HNPost{})
	if !dialect.HasColumn(hnScope.TableName(), "user_id") || !dialect.HasColumn(hnScope.TableName(), "user_name") || !dialect.HasColumn(hnScope.TableName(), "user_email") {
		t.Errorf("should has prefix for embedded columns")
	}
}

func TestSaveAndQueryEmbeddedStruct(t *testing.T) {
	DB.Save(&HNPost{BasePost: BasePost{Id: 1, Title: "news"}})
	DB.Save(&HNPost{BasePost: BasePost{Id: 2, Title: "hn_news"}})
	var news HNPost
	if err := DB.First(&news, "title = ?", "hn_news").Error; err != nil {
		t.Errorf("no error should happen when query with embedded struct, but got %v", err)
	} else if news.Title != "hn_news" {
		t.Errorf("embedded struct's value should be scanned correctly")
	}

	DB.Save(&EngadgetPost{BasePost: BasePost{Id: 3, Title: "engadget_news"}})
	var egNews EngadgetPost
	if err := DB.First(&egNews, "title = ?", "engadget_news").Error; err != nil {
		t.Errorf("no error should happen when query with embedded struct, but got %v", err)
	} else if egNews.BasePost.Title != "engadget_news" {
		t.Errorf("embedded struct's value should be scanned correctly")
	}

	if DB.NewScope(&HNPost{}).PrimaryField() == nil {
		t.Errorf("primary key with embedded struct should works")
	}

	for _, field := range DB.NewScope(&HNPost{}).Fields() {
		if field.Name == "BasePost" {
			t.Errorf("scope Fields should not contain embedded struct")
		}
	}
}
