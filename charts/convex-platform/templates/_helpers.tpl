{{- define "convex.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "convex.fullname" -}}
{{- default .Release.Name .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "convex.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "convex.labelPrefix" -}}
{{- .Values.labelPrefix | default "convex" -}}
{{- end -}}

{{- define "convex.selectorLabels" -}}
app.kubernetes.io/name: {{ include "convex.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "convex.labels" -}}
helm.sh/chart: {{ include "convex.chart" . }}
{{ include "convex.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: convex
{{- end -}}

{{- define "convex.componentLabels" -}}
{{- $ctx := .ctx -}}
{{ include "convex.labels" $ctx }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "convex.componentSelectorLabels" -}}
{{- $ctx := .ctx -}}
{{ include "convex.selectorLabels" $ctx }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "convex.usherName" -}}{{ include "convex.fullname" . }}-usher{{- end -}}
{{- define "convex.bigbrainName" -}}{{ include "convex.fullname" . }}-bigbrain{{- end -}}

{{- define "convex.controlPlaneEnabled" -}}
{{- if .Values.controlPlane.tokenSecretRef.name -}}true{{- end -}}
{{- end -}}

{{- define "convex.controlPlaneEnv" -}}
{{- if eq (include "convex.controlPlaneEnabled" .) "true" }}
- name: BIGBRAIN_CONTROL_PLANE_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ .Values.controlPlane.tokenSecretRef.name }}
      key: {{ default "control-plane-token" .Values.controlPlane.tokenSecretRef.key }}
{{- end }}
{{- end -}}

{{- define "convex.imagePullSecrets" -}}
{{- with .Values.image.pullSecrets }}
imagePullSecrets:
{{- toYaml . | nindent 0 }}
{{- end }}
{{- end -}}

{{- define "convex-platform.image" -}}
{{- $ctx := .ctx -}}
{{- $spec := .spec -}}
{{- $registry := $ctx.Values.image.registry -}}
{{- $repo := $spec.repository -}}
{{- $tag := $spec.tag | default $ctx.Chart.AppVersion -}}
{{- $ref := $repo -}}
{{- if $registry -}}
{{- $ref = printf "%s/%s" $registry $repo -}}
{{- end -}}
{{- printf "%s:%s" $ref $tag -}}
{{- end -}}
