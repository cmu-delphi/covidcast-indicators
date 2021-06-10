## Tools for parsing .qsf files

### `qsf-differ`

Compare two .qsf files to find survey items that have been added, removed, or
changed. This tool returns a list of item names and Qualtrics question IDs
(QIDs) by change type.

### `generate-codebook`

Process one .qsf file at a time to create a new codebook or add to an existing
codebook. This tool processes a qsf to retrieve information about a survey item
text, format, and randomization. Survey items' `shortname`s and which items they
replace, if any, are set manually and need to be updated for every new survey
wave.

### Background on .qsf files

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