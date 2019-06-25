package dataframe

import (
	"github.com/go-bullseye/bullseye/iterator"
)

// StepValueElementAt gets the value at i from the StepValue and casts it to an Element.
func StepValueElementAt(stepValue *iterator.StepValue, i int) Element {
	stepValueEl, dtype := stepValue.Value(i)
	return CastElement(dtype, stepValueEl)
}
