package db

type SQLExpression struct {
	Expression string
}

func (se SQLExpression) String() (s string) {
	for _, c := range se.Expression {
		if c == ':' {
			s = s + "::"
		} else {
			s = s + string(c)
		}
	}
	return s
}
