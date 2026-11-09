for p in /proc/[0-9]*; do
  c=$(tr '\0' ' ' < $p/cmdline 2>/dev/null) || continue
  case "$c" in
    /usr/local/bin/oxia-load-*|oxia-load-*) echo "${p#/proc/}"; exit 0 ;;
  esac
done
exit 1
