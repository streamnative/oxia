{{/*
Common labels
*/}}
{{- define "oxia-controller.labels" -}}
{{ include "oxia-controller.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "oxia-controller.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/component: controller
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Probe
*/}}
{{- define "oxia-controller.probe" -}}
exec:
  command:
    - /bin/grpc-health-probe
    - -addr=:{{ . }}
timeoutSeconds: 10
{{- end }}
