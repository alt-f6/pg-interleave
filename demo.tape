Output demo.gif

Set FontSize 15
Set FontFamily "Cascadia Code"
Set Width 960
Set Height 700
Set Padding 20
Set Theme "Catppuccin Mocha"
Set WindowBar Colorful
Set LetterSpacing 0

Hide
Type "cd packages/sdk-ts"
Enter
Type "cls"
Enter
Sleep 500ms
Show

Type "npx vitest run src/fuzzer.test.ts"
Enter
Sleep 4.5s

Type "npx tsx src/demo.ts"
Enter
Sleep 7.5s

Sleep 3s