package api

import "testing"

// PrintSpec dereferences aep.ResponsePossibilities five times -- four in the
// MarkDown branch and once in PostmanCollection -- and never checked it for nil.
// NewEndPoint has no in-library callers, so that pointer arrives entirely from
// downstream apps with nothing validating it.
//
// A nil therefore panicked, and because APIHandlerPrintSpec serves PrintSpec
// over HTTP the panic took out the whole GET /spec document, not one entry. The
// bpm7 product had to pass an empty set at 32 registration sites to avoid it.
//
// Both output formats are exercised, because the Postman deref sits about sixty
// lines below the other four and guarding only the first block would leave it.
func TestPrintSpecSurvivesNilResponsePossibilities(t *testing.T) {
	original := SpecFormat
	t.Cleanup(func() { SpecFormat = original })

	for _, format := range []string{"MarkDown", "PostmanCollection"} {
		SpecFormat = format

		aep := &DXAPIEndPoint{
			Title:                 "Endpoint declaring no responses",
			Description:           "d",
			Uri:                   "/x",
			Method:                "POST",
			EndPointType:          EndPointTypeHTTPJSON,
			ResponsePossibilities: nil,
		}

		s, err := aep.PrintSpec()
		if err != nil {
			t.Errorf("%s: unexpected error: %v", format, err)
		}
		if s == "" {
			t.Errorf("%s: produced no output", format)
		}
	}
}
