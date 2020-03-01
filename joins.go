package sqb

// JoinBuilder to make joins less painfull
type JoinBuilder struct {
	Joinable
}

func JB(a Joinable) JoinBuilder {
	return JoinBuilder{Joinable: a}
}

func (jb JoinBuilder) LeftJoin(arg Joinable, on OnExpr) JoinBuilder {
	return JoinBuilder{Joinable: LeftJoin(jb.Joinable, arg, on)}
}

func (jb JoinBuilder) RightJoin(arg Joinable, on OnExpr) JoinBuilder {
	return JoinBuilder{Joinable: RightJoin(jb.Joinable, arg, on)}
}

func (jb JoinBuilder) InnerJoin(arg Joinable, on OnExpr) JoinBuilder {
	return JoinBuilder{Joinable: InnerJoin(jb.Joinable, arg, on)}
}

func (jb JoinBuilder) FullOuterJoin(arg Joinable, on OnExpr) JoinBuilder {
	return JoinBuilder{Joinable: FullOuterJoin(jb.Joinable, arg, on)}
}

func (jb JoinBuilder) CrossJoin(arg Joinable, on OnExpr) JoinBuilder {
	return JoinBuilder{Joinable: CrossJoin(jb.Joinable, arg)}
}
