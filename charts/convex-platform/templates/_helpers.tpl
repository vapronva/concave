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

{{- define "convex.bigbrainDeployments" -}}
{{- $pairs := list -}}
{{- range .Values.deployments -}}
{{- $name := required "deployments[].name is required" .name -}}
{{- $namespace := required "deployments[].namespace is required" .namespace -}}
{{- $pairs = append $pairs (printf "%s=%s" $name $namespace) -}}
{{- end -}}
{{- join "," $pairs -}}
{{- end -}}

{{- define "convex.imagePullSecrets" -}}
{{- with .Values.image.pullSecrets -}}
imagePullSecrets:
  {{- toYaml . | nindent 2 }}
{{- end -}}
{{- end -}}

{{- define "convex.listenPort" -}}
{{- splitList ":" . | last -}}
{{- end -}}

{{- define "convex.podScheduling" -}}
{{- $out := list -}}
{{- with .nodeSelector -}}
{{- $out = append $out (printf "nodeSelector:\n%s" (toYaml . | indent 2)) -}}
{{- end -}}
{{- with .tolerations -}}
{{- $out = append $out (printf "tolerations:\n%s" (toYaml . | indent 2)) -}}
{{- end -}}
{{- with .topologySpreadConstraints -}}
{{- $out = append $out (printf "topologySpreadConstraints:\n%s" (toYaml . | indent 2)) -}}
{{- end -}}
{{- join "\n" $out -}}
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
