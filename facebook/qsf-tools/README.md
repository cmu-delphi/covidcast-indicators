## Tools for parsing .qsf files

### `qsf-differ`

Compare two .qsf files to find survey items that have been added, removed, or
changed. This tool returns a list of item names and Qualtrics question IDs
(QIDs) by change type.

### `generate-codebook`

Process one .qsf file at a time to create a new codebook or add to an existing
codebook (in CSV format). This tool processes a qsf to retrieve information
about a survey item text, format, and randomization.

##### Dependencies

The following files are used to define fields that cannot be parsed from the
.qsf, for example, which new survey questions replace which old survey
questions. These mapping files are created manually and need to be updated for
every new survey wave.

* `item_replacement_map.csv`: Lists in-survey name of an `new_item` and the
  in-survey name(s) of the `old_item`(s) it replaces. `new_item` should be the
  name of a single item and be unique; the `old_item` column should be a
  string. However, `old_item` has no other formatting requirements. For
  example, it can list several item names (e.g. "A1, A2"), if the
  corresponding new survey item is replacing multiple old questions. A given
  item name can also appear in multiple rows of the `old_item` field.
* `item_shortquestion_map.csv`: Lists in-survey name of an `item` and a short
  description of the contents of the question. `item` should be the name of a
  single item and be unique, but the `description` column has no formatting
  requirements.
* `static_microdata_fields.csv`: Lists additional fields that are included in
  the microdata but are not derived from the .qsf file. Columns that appear in
  the codebook but not in `static_microdata_fields.csv` are marked as missing
  and filled with `NA`. This item will rarely need to be updated.

### .qsf Background

A .qsf file is a Qualtrics-specific json containing two elements: SurveyEntry
with survey metadata (start date, ID, name, etc) and SurveyElements with a list
of survey components. To catalogue included items by survey wave, we would
examine the SurveyElements object.

SurveyElement types are BL (block), FL (flow), SO (survey options), SCO
(scoring), PROJ (?), notes, STAT (statistics), QC (question count), SQ (survey
questions), and RS (response set). [Detailed info
here](https://gist.github.com/ctesta01/d4255959dace01431fb90618d1e8c241).

The SQ (survey question) objects contain most of the information we're
interested in. Within each SQ item, details are stored in the Payload element,
which can contain any subset of the following fields:

* `QuestionText`
* `QuestionType`
* `Selector`
* `Configuration`
* `QuestionDescription`
* `Choices`
* `ChoiceOrder`
* `Validation`
* `AnalyzeChoices`
* `Language`
* `QuestionText_Unsafe`
* `DataExportTag`
* `QuestionID`
* `DataVisibility`
* `NextChoiceId`
* `NextAnswerId`
* `DefaultChoices`
* `SubSelector`
* `DisplayLogic`
* `GradingData`
* `Answers`
* `AnswerOrder`
* `ChoiceDataExportTags`
* `Randomization`
* `RecodeValues`
* `DynamicChoices`
* `DynamicChoicesData`
* `SearchSource`
* `QuestionJS`

The meaning of "Answers" and "Choices" differs for matrix vs non-matrix
items. "Choices" list the vertical components -- subquestions for matrix
items and answer choices for non-matrix items. "Answers" list the answer
choices for matrix items and are missing for non-matrix items.
