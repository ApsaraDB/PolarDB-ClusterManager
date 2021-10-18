package decision

import (
	"encoding/json"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
)

type PredicatorOperator string
type PredicatorRes string
type PredicatorType string

const (
	PredicatorDirectFail PredicatorRes = "direct_fail"
	PredicatorTrue       PredicatorRes = "true"
	PredicatorFalse      PredicatorRes = "false"

	PredicatorIntType        PredicatorType = "int"
	PredicatorStringType     PredicatorType = "string"
	PredicatorKeyArrayType   PredicatorType = "key_array"
	PredicatorValueArrayType PredicatorType = "value_array"
	PredicatorBoolType       PredicatorType = "bool"

	PredicatorEqualOp    PredicatorOperator = "equal"
	PredicatorNotEqualOp PredicatorOperator = "not"
	PredicatorLargeOp    PredicatorOperator = "larger"
	PredicatorLowerOp    PredicatorOperator = "lower"
	PredicatorInOp       PredicatorOperator = "in"
	PredicatorFailOp     PredicatorOperator = "direct_fail"

	HaDecisionRoute string = "ha_decision_route"
)

type DecisionPath struct {
	ID    int                  `json:"id,omitempty"`
	Nodes []DecisionPredicator `json:"path,omitempty"`
}

type DecisionRoute struct {
	LastID int                  `json:"last_id"`
	Paths  map[int]DecisionPath `json:"paths,omitempty"`
}

type DecisionPredicator struct {
	Key   string             `json:"key,omitempty"`
	Value interface{}        `json:"value,omitempty"`
	Op    PredicatorOperator `json:"op"`
	Type  PredicatorType     `json:"type,omitempty"`
}

type PredicatorResult struct {
	res PredicatorRes
	err error
}

func (p *DecisionPredicator) String() string {
	b, err := json.Marshal(p)
	if err != nil {
		return err.Error()
	} else {
		return string(b)
	}
}

func (p *DecisionPredicator) Evaluate(metrics map[string]interface{}) PredicatorResult {
	if p.Op == PredicatorFailOp {
		return PredicatorResult{
			res: PredicatorDirectFail,
		}
	}
	if v, exist := metrics[p.Key]; exist {
		if p.Type == PredicatorStringType {
			keyV, ok := v.(string)
			if !ok {
				return PredicatorResult{
					err: errors.Errorf("metrics %s value %v not string", p.Key, v),
				}
			}
			constant, ok := p.Value.(string)
			if !ok {
				return PredicatorResult{
					err: errors.Errorf("metrics %s value %v not string", p.Key, p.Value),
				}
			}

			switch p.Op {
			case PredicatorEqualOp:
				if keyV == constant {
					return PredicatorResult{
						res: PredicatorTrue,
					}
				} else {
					return PredicatorResult{
						res: PredicatorFalse,
					}
				}
			default:
				return PredicatorResult{
					err: errors.Errorf("op %s not support", p.Op),
				}
			}
		} else if p.Type == PredicatorBoolType {
			keyV, ok := v.(bool)
			if !ok {
				return PredicatorResult{
					err: errors.Errorf("metrics %s value %v not bool", p.Key, v),
				}
			}
			constant, ok := p.Value.(bool)
			if !ok {
				return PredicatorResult{
					err: errors.Errorf("metrics %s value %v not bool", p.Key, p.Value),
				}
			}

			switch p.Op {
			case PredicatorEqualOp:
				if keyV == constant {
					return PredicatorResult{
						res: PredicatorTrue,
					}
				} else {
					return PredicatorResult{
						res: PredicatorFalse,
					}
				}
			case PredicatorNotEqualOp:
				if keyV != constant {
					return PredicatorResult{
						res: PredicatorTrue,
					}
				} else {
					return PredicatorResult{
						res: PredicatorFalse,
					}
				}
			default:
				return PredicatorResult{
					err: errors.Errorf("op %s not support", p.Op),
				}
			}
		} else if p.Type == PredicatorIntType {
			keyV, ok := v.(int)
			if !ok {
				return PredicatorResult{
					err: errors.Errorf("metrics %s value %v not int", p.Key, v),
				}
			}
			constant, ok := p.Value.(int)
			if !ok {
				return PredicatorResult{
					err: errors.Errorf("metrics %s value %v not int", p.Key, p.Value),
				}
			}

			switch p.Op {
			case PredicatorEqualOp:
				if keyV == constant {
					return PredicatorResult{
						res: PredicatorTrue,
					}
				} else {
					return PredicatorResult{
						res: PredicatorFalse,
					}
				}
			case PredicatorNotEqualOp:
				if keyV != constant {
					return PredicatorResult{
						res: PredicatorTrue,
					}
				} else {
					return PredicatorResult{
						res: PredicatorFalse,
					}
				}
			case PredicatorLargeOp:
				if keyV > constant {
					return PredicatorResult{
						res: PredicatorTrue,
					}
				} else {
					return PredicatorResult{
						res: PredicatorFalse,
					}
				}
			case PredicatorLowerOp:
				if keyV < constant {
					return PredicatorResult{
						res: PredicatorTrue,
					}
				} else {
					return PredicatorResult{
						res: PredicatorFalse,
					}
				}
			default:
				return PredicatorResult{
					err: errors.Errorf("op %s not support", p.Op),
				}
			}

		} else if p.Type == PredicatorKeyArrayType {
			keyV, ok := v.([]string)
			if !ok {
				return PredicatorResult{
					err: errors.Errorf("metrics %s key %v not array", p.Key, v),
				}
			}
			valueV, ok := p.Value.(string)
			if !ok {
				return PredicatorResult{
					err: errors.Errorf("metrics %s value %v not string", p.Key, v),
				}
			}

			switch p.Op {
			case PredicatorInOp:
				if CheckLastNReason(keyV, valueV, 3) {
					return PredicatorResult{
						res: PredicatorTrue,
					}
				} else {
					return PredicatorResult{
						res: PredicatorFalse,
					}
				}
			default:
				return PredicatorResult{
					err: errors.Errorf("op %s not support", p.Op),
				}
			}
		} else if p.Type == PredicatorValueArrayType {
			keyV, ok := v.(string)
			if !ok {
				return PredicatorResult{
					err: errors.Errorf("metrics %s value %v not string", p.Key, v),
				}
			}
			constant, ok := p.Value.([]string)
			if !ok {
				return PredicatorResult{
					err: errors.Errorf("metrics %s value %v not array", p.Key, v),
				}
			}

			switch p.Op {
			case PredicatorInOp:
				if CheckLastNReason(constant, keyV, len(constant)) {
					return PredicatorResult{
						res: PredicatorTrue,
					}
				} else {
					return PredicatorResult{
						res: PredicatorFalse,
					}
				}
			default:
				return PredicatorResult{
					err: errors.Errorf("op %s not support", p.Op),
				}
			}

		} else {
			return PredicatorResult{
				err: errors.Errorf("metrics %s type %s not support", p.Key, p.Type),
			}
		}
	} else {
		return PredicatorResult{
			res: PredicatorFalse,
			err: errors.Errorf("metrics %s not exist", p.Key),
		}
	}

}

func (t *DecisionRoute) GetDecisionPath() (string, error) {
	v, err := json.Marshal(t)
	if err != nil {
		return "", err
	} else {
		return string(v), nil
	}
}

func (t *DecisionRoute) ValidateDecisionPath(path DecisionPath) error {
	return nil
}

func (t *DecisionRoute) AddDecisionPath(path DecisionPath) error {
	if err := t.ValidateDecisionPath(path); err != nil {
		return err
	}

	t.LastID++
	t.Paths[t.LastID] = path

	v, err := json.Marshal(t)
	if err != nil {
		return errors.Wrapf(err, "Failed to marshal #{path}")
	}

	return meta.GetMetaManager().AddCmConf(HaDecisionRoute, string(v))
}

func (t *DecisionRoute) RemoveDecisionPath(id int) error {
	delete(t.Paths, id)

	v, err := json.Marshal(t)
	if err != nil {
		return errors.Wrapf(err, "Failed to marshal #{path}")
	}

	return meta.GetMetaManager().AddCmConf(HaDecisionRoute, string(v))
}
