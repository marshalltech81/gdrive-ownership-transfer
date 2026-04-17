# VHS tape — animated terminal demo for gdrive-ownership-transfer
# Run with: vhs demo.tape

Output demo.gif

Require gdrive-ownership-transfer

Set Shell "bash"
Set FontSize 14
Set Width 1200
Set Height 600
Set Theme "Dracula"
Set Padding 20

# --- version ---
Type "gdrive-ownership-transfer --version"
Enter
Sleep 1s

# --- help ---
Type "gdrive-ownership-transfer --help"
Enter
Sleep 2s

# --- scan (dry-run, no real credentials) ---
Type "# 1. Scan a shared folder tree (dry run)"
Enter
Sleep 500ms
Type "gdrive-ownership-transfer scan \\"
Enter
Type "  --folder-id 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms \\"
Enter
Type "  --credentials-file credentials.json"
Enter
Sleep 2s

# --- request (dry-run) ---
Type "# 2. Send transfer requests — dry run first"
Enter
Sleep 500ms
Type "gdrive-ownership-transfer request \\"
Enter
Type "  --folder-id 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms \\"
Enter
Type "  --target-email recipient@gmail.com \\"
Enter
Type "  --credentials-file credentials.json"
Enter
Sleep 2s

# --- doctor ---
Type "# 3. Run diagnostics"
Enter
Sleep 500ms
Type "gdrive-ownership-transfer doctor \\"
Enter
Type "  --folder-id 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms \\"
Enter
Type "  --credentials-file credentials.json"
Enter
Sleep 2s

# --- diff ---
Type "# 4. Compare two report CSVs"
Enter
Sleep 500ms
Type "gdrive-ownership-transfer diff before.csv after.csv"
Enter
Sleep 2s

Sleep 1s
