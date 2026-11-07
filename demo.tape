# Converge - Split-Brain Simulation Demo Tape
# Powered by VHS (https://github.com/charmbracelet/vhs)
#
# To render the demo GIF:
#   vhs demo.tape

Output assets/demo.gif

Set FontSize 12
Set Width 1140
Set Height 820
Set Theme "TokyoNight"
Set Padding 16
Set FontFamily "JetBrains Mono"
Set LetterSpacing 0
Set LineHeight 1.2

Type ".\converge.exe demo --auto --delay 850ms"
Sleep 600ms
Enter

# Wait for 5 phases: Bootstrap, Replication, Split-Brain, Healing, SQLite Disk Verification
Sleep 19s
