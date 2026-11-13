package chversion

import "testing"

func TestParseAndCompare(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"25.3.3.42", "22.3", 1},
		{"21.8.1.1", "21.8", 0},
		{"22.2.3.5", "22.3", -1},
		{"25.3", "25.3.0.0", 0},
	}
	for _, c := range cases {
		va, err := Parse(c.a)
		if err != nil {
			t.Fatalf("Parse(%q): %v", c.a, err)
		}
		vb, err := Parse(c.b)
		if err != nil {
			t.Fatalf("Parse(%q): %v", c.b, err)
		}
		if got := Cmp(va, vb); got != c.want {
			t.Fatalf("Cmp(%q, %q) = %d, ожидается %d", c.a, c.b, got, c.want)
		}
	}
	if _, err := Parse("25"); err == nil {
		t.Fatal("Parse(одна разрядка) обязан падать")
	}
}

func TestGates(t *testing.T) {
	v, _ := Parse("25.3.3.42")
	since, _ := Parse("22.3")
	until, _ := Parse("26.0")
	if !Gte(v, since) || !Lte(v, until) {
		t.Fatal("гейты since/until")
	}
	low, _ := Parse("21.8")
	if Gte(low, since) {
		t.Fatal("since 22.3 не должен пропускать 21.8")
	}
}
