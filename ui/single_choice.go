package ui

import (
	"fmt"
	"log"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
)

// Will prompt the user to select an option of the provided choices and choice their selection. If true is returned
// it means the user quit instead of selecting an option. The provided message can be used to display text to the user
// as part of the prompt, but is optional.
func PromptUserForSingleChoice(choices []string, message string) (string, bool) {
	model := singleChoiceModel{
		choices: choices,
		message: message,
	}

	p := tea.NewProgram(model)
	res, err := p.Run()
	if err != nil {
		log.Fatal(err)
	}

	if m, ok := res.(singleChoiceModel); ok {
		if m.quit {
			return "", true
		}

		return m.selected, false
	}

	return "", false
}

type singleChoiceModel struct {
	choices  []string
	cursor   int
	selected string
	message  string
	quit     bool
}

func (m singleChoiceModel) Init() tea.Cmd {
	return nil
}

func (m singleChoiceModel) Update(incomingMsg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := incomingMsg.(type) {
	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyCtrlC, tea.KeyEsc:
			m.quit = true
			return m, tea.Quit
		case tea.KeyUp:
			if m.cursor > 0 {
				m.cursor--
			}
		case tea.KeyDown:
			if m.cursor < len(m.choices)-1 {
				m.cursor++
			}
		case tea.KeyEnter:
			m.selected = m.choices[m.cursor]
			return m, tea.Quit
		}
	default:
	}

	return m, nil
}

func (m singleChoiceModel) View() string {
	s := strings.Builder{}

	if m.message != "" {
		s.Write([]byte(fmt.Sprintf("%s\n\n", m.message)))
	}

	for i := 0; i < len(m.choices); i++ {
		if m.cursor == i {
			s.WriteString("(•) ")
		} else {
			s.WriteString("( ) ")
		}
		s.WriteString(m.choices[i])
		s.WriteString("\n")
	}
	s.WriteString("\n(press esc to quit)\n")

	return s.String()
}
