{{/*
Coordinator labels
*/}}
{{- define "oxia-cluster.coordinator.labels" -}}
{{ include "oxia-cluster.coordinator.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/part-of: oxia
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Coordinator selector labels
*/}}
{{- define "oxia-cluster.coordinator.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/component: coordinator
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Server labels
*/}}
{{- define "oxia-cluster.server.labels" -}}
{{ include "oxia-cluster.server.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Server selector labels
*/}}
{{- define "oxia-cluster.server.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/component: server
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Probe
*/}}
{{- define "oxia-cluster.probe" -}}
grpc:
  port: {{ . }}
timeoutSeconds: 10
{{- end }}
