package main

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// UI controls terminal styling, colors, and box-drawing primitives.
type UI struct {
	noColor bool
}

// NewUI creates a UI renderer with optional ANSI color suppression.
func NewUI(noColor bool) *UI {
	return &UI{noColor: noColor}
}

// ANSI Escape Codes
const (
	ansiReset   = "\033[0m"
	ansiBold    = "\033[1m"
	ansiDim     = "\033[2m"
	ansiRed     = "\033[31m"
	ansiGreen   = "\033[32m"
	ansiYellow  = "\033[33m"
	ansiBlue    = "\033[34m"
	ansiMagenta = "\033[35m"
	ansiCyan    = "\033[36m"
	ansiWhite   = "\033[37m"
	ansiGray    = "\033[90m"
)

func (u *UI) color(code, text string) string {
	if u.noColor {
		return text
	}
	return code + text + ansiReset
}

func (u *UI) Bold(text string) string    { return u.color(ansiBold, text) }
func (u *UI) Dim(text string) string     { return u.color(ansiDim, text) }
func (u *UI) Red(text string) string     { return u.color(ansiRed, text) }
func (u *UI) Green(text string) string   { return u.color(ansiGreen, text) }
func (u *UI) Yellow(text string) string  { return u.color(ansiYellow, text) }
func (u *UI) Magenta(text string) string { return u.color(ansiMagenta, text) }
func (u *UI) Cyan(text string) string    { return u.color(ansiCyan, text) }
func (u *UI) White(text string) string   { return u.color(ansiWhite, text) }
func (u *UI) Gray(text string) string    { return u.color(ansiGray, text) }

// Banner renders a styled header block.
func (u *UI) Banner(title, subtitle string) {
	width := 76
	top := "╭" + strings.Repeat("─", width-2) + "╮"
	bot := "╰" + strings.Repeat("─", width-2) + "╯"

	fmt.Println()
	fmt.Println(u.Cyan(top))
	fmt.Printf("%s  %s%s%s\n",
		u.Cyan("│"),
		u.Bold(u.White(title)),
		strings.Repeat(" ", width-4-utf8.RuneCountInString(title)),
		u.Cyan("│"),
	)
	if subtitle != "" {
		fmt.Printf("%s  %s%s%s\n",
			u.Cyan("│"),
			u.Dim(subtitle),
			strings.Repeat(" ", width-4-utf8.RuneCountInString(subtitle)),
			u.Cyan("│"),
		)
	}
	fmt.Println(u.Cyan(bot))
	fmt.Println()
}

// PhaseHeader renders a phase milestone banner.
func (u *UI) PhaseHeader(current, total int, title string) {
	width := 76
	tag := fmt.Sprintf(" PHASE %d/%d: %s ", current, total, strings.ToUpper(title))
	tagLen := utf8.RuneCountInString(tag)

	rightLen := width - 4 - tagLen
	if rightLen < 2 {
		rightLen = 2
	}

	header := "╭─" + u.Bold(u.Cyan(tag)) + strings.Repeat("─", rightLen) + "╮"
	fmt.Println()
	fmt.Println(header)
}

// PhaseEnd closes a phase box.
func (u *UI) PhaseEnd() {
	width := 76
	fmt.Println("╰" + strings.Repeat("─", width-2) + "╯")
	fmt.Println()
}

// CardLine renders an indented line inside a section.
func (u *UI) CardLine(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	fmt.Printf("│  %s\n", msg)
}

// Bullet renders a labeled status bullet.
func (u *UI) Bullet(colorFn func(string) string, label, format string, a ...interface{}) {
	desc := fmt.Sprintf(format, a...)
	fmt.Printf("│  %s %-12s %s\n", colorFn("●"), u.Bold(label), desc)
}

// StepAction renders an executed cluster action.
func (u *UI) StepAction(replicaID, format string, a ...interface{}) {
	action := fmt.Sprintf(format, a...)
	badge := u.Bold(u.Cyan(fmt.Sprintf("[%s]", replicaID)))
	fmt.Printf("│  %s %s %s\n", u.Dim("↳"), badge, action)
}

// PartitionDiagram prints an ASCII visualization of network islands.
func (u *UI) PartitionDiagram(islandA, islandB []string) {
	fmt.Println("│")
	fmt.Printf("│  %s\n", u.Yellow("⚡ Symmetrical Network Partition Injected ⚡"))
	fmt.Printf("│    %s  %s  %s\n",
		u.Green(fmt.Sprintf("Island A: { %s }", strings.Join(islandA, ", "))),
		u.Red("◄──[ BLOCKED ]──►"),
		u.Yellow(fmt.Sprintf("Island B: { %s } (isolated)", strings.Join(islandB, ", "))),
	)
	fmt.Println("│")
}

// HealingDiagram prints an ASCII visualization of restored connectivity.
func (u *UI) HealingDiagram() {
	fmt.Println("│")
	fmt.Printf("│  %s %s\n",
		u.Green("✓"),
		u.Bold(u.Green("Network Links Restored · Transitive Mesh Reconnected")),
	)
	fmt.Printf("│  %s\n", u.Dim("Initiating bidirectional watermark reconciliation across all peer sessions..."))
	fmt.Println("│")
}

// Table renders a Unicode box-drawing table.
func (u *UI) Table(headers []string, rows [][]string) {
	numCols := len(headers)
	colWidths := make([]int, numCols)

	for i, h := range headers {
		colWidths[i] = utf8.RuneCountInString(h)
	}

	for _, row := range rows {
		for i, cell := range row {
			if i < numCols {
				w := utf8.RuneCountInString(stripAnsi(cell))
				if w > colWidths[i] {
					colWidths[i] = w
				}
			}
		}
	}

	// Top border
	var topParts []string
	for _, w := range colWidths {
		topParts = append(topParts, strings.Repeat("─", w+2))
	}
	fmt.Printf("│  ┌%s┐\n", strings.Join(topParts, "┬"))

	// Header
	var headParts []string
	for i, h := range headers {
		pad := colWidths[i] - utf8.RuneCountInString(h)
		headParts = append(headParts, fmt.Sprintf(" %s%s ", u.Bold(h), strings.Repeat(" ", pad)))
	}
	fmt.Printf("│  │%s│\n", strings.Join(headParts, "│"))

	// Separator
	var sepParts []string
	for _, w := range colWidths {
		sepParts = append(sepParts, strings.Repeat("─", w+2))
	}
	fmt.Printf("│  ├%s┤\n", strings.Join(sepParts, "┼"))

	// Rows
	for _, row := range rows {
		var rowParts []string
		for i := 0; i < numCols; i++ {
			cell := ""
			if i < len(row) {
				cell = row[i]
			}
			rawLen := utf8.RuneCountInString(stripAnsi(cell))
			pad := colWidths[i] - rawLen
			if pad < 0 {
				pad = 0
			}
			rowParts = append(rowParts, fmt.Sprintf(" %s%s ", cell, strings.Repeat(" ", pad)))
		}
		fmt.Printf("│  │%s│\n", strings.Join(rowParts, "│"))
	}

	// Bottom border
	var botParts []string
	for _, w := range colWidths {
		botParts = append(botParts, strings.Repeat("─", w+2))
	}
	fmt.Printf("│  └%s┘\n", strings.Join(botParts, "┴"))
}

// SummaryBox renders a concluding success summary.
func (u *UI) SummaryBox(title string, takeaways []string) {
	width := 76
	top := "╭" + strings.Repeat("─", width-2) + "╮"
	bot := "╰" + strings.Repeat("─", width-2) + "╯"

	fmt.Println()
	fmt.Println(u.Green(top))
	fmt.Printf("%s  %s%s%s\n",
		u.Green("│"),
		u.Bold(u.Green("✓ "+title)),
		strings.Repeat(" ", width-6-utf8.RuneCountInString(title)),
		u.Green("│"),
	)
	fmt.Println(u.Green("├" + strings.Repeat("─", width-2) + "┤"))
	for i, t := range takeaways {
		line := fmt.Sprintf("%d. %s", i+1, t)
		pad := width - 4 - utf8.RuneCountInString(line)
		if pad < 0 {
			pad = 0
		}
		fmt.Printf("%s  %s%s%s\n", u.Green("│"), line, strings.Repeat(" ", pad), u.Green("│"))
	}
	fmt.Println(u.Green(bot))
	fmt.Println()
}

func stripAnsi(str string) string {
	var b strings.Builder
	inEscape := false
	for i := 0; i < len(str); i++ {
		if str[i] == '\033' {
			inEscape = true
			continue
		}
		if inEscape {
			if str[i] == 'm' {
				inEscape = false
			}
			continue
		}
		b.WriteByte(str[i])
	}
	return b.String()
}
