package domain

import (
	"fmt"
	"github.com/charmbracelet/lipgloss"
	"os"
)

const (
	Red         Color = "#cc0000"
	LightBlue   Color = "#3cc5ff"
	Blue        Color = "#0c00cc"
	Yellow      Color = "#cc9500"
	LightOrange Color = "#ff9a59"
	Orange      Color = "#ff7c28"
	LightGreen  Color = "#06ff00"
	Green       Color = "#06cc00"
	DarkGreen   Color = "#055c03"
	LightPurple Color = "#d864ff"
	Purple      Color = "#7400e0"
	Grey        Color = "#adadad"
)

var (
	// ColorDisabled determines whether ANSI colored text output should be disabled.
	//
	// By default, ColorDisabled is set to false. The init function of this package (the one defined in this file)
	// checks if a NO_COLOR environment variable is set. If so, then colored output is disabled.
	//
	// https://no-color.org/
	ColorDisabled bool = false

	RedStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color(Red))
	LightOrangeStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color(LightOrange))
	OrangeStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color(Orange))
	YellowStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color(Yellow))
	LightGreenStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color(LightGreen))
	GreenStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color(Green))
	DarkGreenStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color(DarkGreen))
	LightBlueStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color(LightBlue))
	BlueStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color(Blue))
	LightPurpleStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color(LightPurple))
	PurpleStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color(Purple))
	GrayStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color(Grey))

	ColorToColorStyle = map[Color]lipgloss.Style{
		Red:         RedStyle,
		LightOrange: LightOrangeStyle,
		Orange:      OrangeStyle,
		Yellow:      YellowStyle,
		LightGreen:  LightGreenStyle,
		Green:       GreenStyle,
		DarkGreen:   DarkGreenStyle,
		LightBlue:   LightBlueStyle,
		Blue:        BlueStyle,
		LightPurple: LightPurpleStyle,
		Purple:      PurpleStyle,
		Grey:        GrayStyle,
	}
)

type Color string

func (c Color) String() string {
	return string(c)
}

func init() {
	noColorVal := os.Getenv("NO_COLOR")
	if noColorVal != "" {
		fmt.Printf("[INFO] Found non-empty value for \"NO_COLOR\" environment variable: \"%s\".\n"+
			"Disabling colored output.\n", noColorVal)
		ColorDisabled = true
	} else {
		fmt.Println("[INFO] Colored log output is enabled.")
	}
}

// ColorizeText colorizes the given text using ANSI color codes.
// Specifically, ColorizeText first prepends the given text with the ANSI color code for the requested color to the
// given string. ColorizeText also appends the ANSI 'stop' code to the end of the string so that the colored text stops.
func ColorizeText(text string, color Color) string {
	// If colored text is disabled, then return the string as-is.
	if ColorDisabled {
		return text
	}

	style := ColorToColorStyle[color]
	return style.Render(text)
}
