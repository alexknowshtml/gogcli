package cmd

import (
	"testing"

	"google.golang.org/api/people/v1"
)

func TestContactsPresentationSchemas(t *testing.T) {
	t.Parallel()

	person := &people.Person{
		ResourceName:   "people/123",
		Names:          []*people.Name{{DisplayName: "Ada\tLovelace"}},
		EmailAddresses: []*people.EmailAddress{{Value: "ada@example.com"}},
		PhoneNumbers:   []*people.PhoneNumber{{Value: "+1\t555"}},
		Birthdays:      []*people.Birthday{{Text: "June\t12"}},
	}

	t.Run("contacts", func(t *testing.T) {
		t.Parallel()
		got := renderPlainTable(t, []*people.Person{person}, contactColumns())
		assertTableOutput(
			t,
			got,
			"RESOURCE\tNAME\tEMAIL\tPHONE\tBIRTHDAY\n"+
				"people/123\tAda Lovelace\tada@example.com\t+1 555\tJune 12\n",
		)
	})

	t.Run("directory people", func(t *testing.T) {
		t.Parallel()
		got := renderPlainTable(t, []*people.Person{person}, directoryPersonColumns())
		assertTableOutput(t, got, "RESOURCE\tNAME\tEMAIL\npeople/123\tAda Lovelace\tada@example.com\n")
	})

	t.Run("other contacts", func(t *testing.T) {
		t.Parallel()
		got := renderPlainTable(t, []*people.Person{person}, otherContactColumns())
		assertTableOutput(
			t,
			got,
			"RESOURCE\tNAME\tEMAIL\tPHONE\npeople/123\tAda Lovelace\tada@example.com\t+1 555\n",
		)
	})

	t.Run("relations", func(t *testing.T) {
		t.Parallel()
		got := renderPlainTable(t, []*people.Relation{
			{FormattedType: "line\tmanager", Person: "people/456"},
		}, peopleRelationColumns())
		assertTableOutput(t, got, "TYPE\tPERSON\nline manager\tpeople/456\n")
	})

	t.Run("dedupe", func(t *testing.T) {
		t.Parallel()
		duplicate := &people.Person{
			ResourceName:   "people/456",
			Names:          []*people.Name{{DisplayName: "Ada Duplicate"}},
			EmailAddresses: []*people.EmailAddress{{Value: "ada@example.com"}},
		}
		rows := contactsDedupeRows([]contactsDedupeGroup{{
			Primary:   person,
			Members:   []*people.Person{person, duplicate},
			MatchedOn: []string{"email", "phone"},
		}})
		got := renderPlainTable(t, rows, contactsDedupeColumns())
		assertTableOutput(
			t,
			got,
			"GROUP\tACTION\tRESOURCE\tNAME\tEMAIL\tPHONE\tMATCHED_ON\n"+
				"1\tkeep\tpeople/123\tAda Lovelace\tada@example.com\t+1 555\temail,phone\n"+
				"1\tmerge\tpeople/456\tAda Duplicate\tada@example.com\t\temail,phone\n",
		)
	})
}

func TestContactsPresentationRows(t *testing.T) {
	t.Parallel()

	person := &people.Person{ResourceName: "people/123"}
	rows := compactPeopleRows([]*people.Person{nil, person, nil})
	if len(rows) != 1 || rows[0] != person {
		t.Fatalf("rows = %#v, want only people/123", rows)
	}

	searchRows := contactSearchRows([]*people.SearchResult{
		nil,
		{},
		{Person: person},
	})
	if len(searchRows) != 1 || searchRows[0] != person {
		t.Fatalf("search rows = %#v, want only people/123", searchRows)
	}
}
