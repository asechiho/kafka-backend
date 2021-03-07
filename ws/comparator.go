package ws

import (
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

type Comparator interface {
	Compare(interface{}, interface{}) bool
}

func New(operatorType OperatorType, castValue string) Comparator {
	switch castValue {
	case "int":
		return NumberComparator{operatorType}
	case "string":
		return StringComparator{operatorType}
	default:
		return nil
	}
}

type StringComparator struct {
	operatorType OperatorType
}

func (stringComparator StringComparator) Compare(left, right interface{}) bool {
	log.Debugf("String compare: left - %s, right %s", left.(string), right.(string))

	switch stringComparator.operatorType {
	case OperatorTypeEq:
		return strings.EqualFold(left.(string), right.(string))
	case OperatorTypeNe:
		return !strings.EqualFold(left.(string), right.(string))
	default:
		return true
	}
}

type NumberComparator struct {
	operatorType OperatorType
}

func (numberComparator NumberComparator) Compare(left, right interface{}) bool {
	var (
		err         error
		leftNumber  int64
		rightNumber int64
	)

	if rightNumber, err = strconv.ParseInt(right.(string), 10, 64); err != nil {
		log.Debugf("Filter value %v parse error: %s", right, err.Error())
		return false
	}

	switch left.(type) {
	case int:
		leftNumber = int64(left.(int))
	case int32:
		leftNumber = int64(left.(int32))
	case int64:
		leftNumber = left.(int64)
	default:
		leftNumber = left.(int64)
	}

	log.Debugf("Int compare: left %d, right %d", leftNumber, rightNumber)

	switch numberComparator.operatorType {
	case OperatorTypeEq:
		return leftNumber == rightNumber
	case OperatorTypeNe:
		return leftNumber != rightNumber
	case OperatorTypeGe:
		return leftNumber >= rightNumber
	case OperatorTypeGt:
		return leftNumber > rightNumber
	case OperatorTypeLe:
		return leftNumber <= rightNumber
	case OperatorTypeLt:
		return leftNumber < rightNumber
	default:
		return true
	}
}
