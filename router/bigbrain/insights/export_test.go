package insights

func (i *Insights) MemLen() int {
	i.memMu.Lock()
	defer i.memMu.Unlock()
	total := 0
	for _, dep := range i.mem {
		total += dep.size
	}
	return total
}
