package main

import (
	"context"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	cloudfront "github.com/upbound/provider-aws/v2/apis/cluster/cloudfront/v1beta1"
	bucket "github.com/upbound/provider-aws/v2/apis/cluster/s3/v1beta1"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	"github.com/crossplane/function-sdk-go/errors"
	"github.com/crossplane/function-sdk-go/logging"
	fnv1 "github.com/crossplane/function-sdk-go/proto/v1"
	"github.com/crossplane/function-sdk-go/request"
	"github.com/crossplane/function-sdk-go/resource"
	"github.com/crossplane/function-sdk-go/resource/composed"
	"github.com/crossplane/function-sdk-go/response"
)

// nome lógico de cada recurso na composition (chave do map desired/observed)
const (
	resBucket                        = resource.Name("bucket")
	resBucketOwnershipControls       = resource.Name("bucketOwnershipControls")
	resBucketPublicAccessBlock       = resource.Name("bucketPublicAccessBlock")
	resBucketCorsConfiguration       = resource.Name("bucketCorsConfiguration")
	resBucketWebsiteConfiguration    = resource.Name("bucketWebsiteConfiguration")
	resBucketSSEConfiguration        = resource.Name("bucketServerSideEncryptionConfiguration")
	resBucketPolicy                  = resource.Name("bucketPolicy")
	resOriginAccessControl           = resource.Name("originAccessControl")
	resResponseHeadersPolicy         = resource.Name("responseHeadersPolicy")
	resOriginRequestPolicy           = resource.Name("originRequestPolicy")
	resCachePolicy                   = resource.Name("cachePolicy")
	resDistribution                  = resource.Name("distribution")
	annotationCompositionResourceKey = "crossplane.io/composition-resource-name"
)

// helper pra pegar status.atProvider.id de um composed observado
func getID(obs map[resource.Name]resource.ObservedComposed, name resource.Name) string {
	if o, ok := obs[name]; ok && o.Resource != nil {
		id, _ := o.Resource.GetString("status.atProvider.id")
		return id
	}
	return ""
}

type policySpec struct {
	name        string
	create      bool
	fromXR      string
	observedRes resource.Name
	outPtr      **string
}

func strPtrOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func resolvePolicies(
	observed map[resource.Name]resource.ObservedComposed,
	policies []policySpec,
) (missing []string) {

	for _, p := range policies {
		if p.create {
			id := getID(observed, p.observedRes)
			if id == "" {
				missing = append(missing, p.name)
				*p.outPtr = nil
				continue
			}
			*p.outPtr = strPtrOrNil(id)
			continue
		}

		// não cria: usa o ID vindo do XR (se tiver)
		*p.outPtr = strPtrOrNil(p.fromXR)
	}

	return missing
}

func composedReady(obs map[resource.Name]resource.ObservedComposed, name resource.Name) bool {
	oc, ok := obs[name]
	if !ok || oc.Resource == nil {
		return false
	}

	v, err := oc.Resource.GetValue("status.conditions")
	if err != nil || v == nil {
		return false
	}

	conds, ok := v.([]any)
	if !ok {
		return false
	}

	for _, c := range conds {
		m, ok := c.(map[string]any)
		if !ok {
			continue
		}

		t, _ := m["type"].(string)

		switch s := m["status"].(type) {
		case string:
			if t == "Ready" && strings.EqualFold(s, "true") {
				return true
			}
		case bool:
			if t == "Ready" && s {
				return true
			}
		}
	}

	return false
}

// func isBucketReady(obs map[resource.Name]resource.ObservedComposed) bool {
// 	o, ok := obs[resBucket]
// 	if !ok || o.Resource == nil {
// 		return false
// 	}

// 	arn, _ := o.Resource.GetString("status.atProvider.arn")
// 	return arn != ""
// }

func isDistributionReady(obs map[resource.Name]resource.ObservedComposed) string {
	o, ok := obs[resDistribution]
	if !ok || o.Resource == nil {
		return ""
	}

	id, _ := o.Resource.GetString("status.atProvider.id")
	return id
}

// Function returns whatever response you ask it to.
type Function struct {
	fnv1.UnimplementedFunctionRunnerServiceServer

	log logging.Logger
}

func (f *Function) RunFunction(_ context.Context, req *fnv1.RunFunctionRequest) (*fnv1.RunFunctionResponse, error) {
	f.log.Info("Running Function", "tag", req.GetMeta().GetTag())

	rsp := response.To(req, response.DefaultTTL)
	// XR observado
	xr, err := request.GetObservedCompositeResource(req)
	if err != nil {
		response.ConditionFalse(rsp, "FunctionSuccess", "InternalError").
			WithMessage("Unable to get observed XR").
			TargetComposite()
		response.Warning(rsp, errors.New("unable to get observed XR")).
			TargetComposite()
		response.Fatal(rsp, errors.Wrapf(err, "cannot get observed composite resource from %T", req))
		return rsp, nil
	}

	xrName := xr.Resource.GetName()

	log := f.log.WithValues(
		"xr-version", xr.Resource.GetAPIVersion(),
		"xr-kind", xr.Resource.GetKind(),
		"xr-name", xrName,
	)

	// Campos do XR (Kind: Frontend)
	awsAccountId, err := xr.Resource.GetString("spec.awsAccountId")
	if err != nil {
		response.Fatal(rsp, errors.Wrapf(err, "cannot read spec.awsAccountId field of %s", xr.Resource.GetKind()))
		return rsp, nil
	}
	bucketName, err := xr.Resource.GetString("spec.bucketName")
	if err != nil {
		response.Fatal(rsp, errors.Wrapf(err, "cannot read spec.bucketName field of %s", xr.Resource.GetKind()))
		return rsp, nil
	}
	region, err := xr.Resource.GetString("spec.region")
	if err != nil {
		response.Fatal(rsp, errors.Wrapf(err, "cannot read spec.region field of %s", xr.Resource.GetKind()))
		return rsp, nil
	}
	certificateArn, err := xr.Resource.GetString("spec.certificateArn")
	if err != nil {
		response.Fatal(rsp, errors.Wrapf(err, "cannot read spec.certificateArn field of %s", xr.Resource.GetKind()))
		return rsp, nil
	}
	defaultRootObject, err := xr.Resource.GetString("spec.defaultRootObject")
	if err != nil {
		response.Fatal(rsp, errors.Wrapf(err, "cannot read spec.defaultRootObject field of %s", xr.Resource.GetKind()))
		return rsp, nil
	}
	dns, err := xr.Resource.GetStringArray("spec.dns")
	if err != nil {
		response.Fatal(rsp, errors.Wrapf(err, "cannot read spec.dns field of %s", xr.Resource.GetKind()))
		return rsp, nil
	}
	httpVersion, err := xr.Resource.GetString("spec.httpVersion")
	if err != nil {
		response.Fatal(rsp, errors.Wrapf(err, "cannot read spec.httpVersion field of %s", xr.Resource.GetKind()))
		return rsp, nil
	}
	priceClass, err := xr.Resource.GetString("spec.priceClass")
	if err != nil {
		response.Fatal(rsp, errors.Wrapf(err, "cannot read spec.priceClass field of %s", xr.Resource.GetKind()))
		return rsp, nil
	}
	minimumProtocolVersion, err := xr.Resource.GetString("spec.minimumProtocolVersion")
	if err != nil {
		response.Fatal(rsp, errors.Wrapf(err, "cannot read spec.minimumProtocolVersion field of %s", xr.Resource.GetKind()))
		return rsp, nil
	}
	allowedMethods, err := xr.Resource.GetStringArray("spec.allowedMethods")
	if err != nil {
		response.Fatal(rsp, errors.Wrapf(err, "cannot read spec.allowedMethods field of %s", xr.Resource.GetKind()))
		return rsp, nil
	}

	// flags pra criar policies custom
	createRHP, _ := xr.Resource.GetBool("spec.createResponseHeadersPolicy")
	createORP, _ := xr.Resource.GetBool("spec.createOriginRequestPolicy")
	createCP, _ := xr.Resource.GetBool("spec.createCachePolicy")

	// IDs vindos diretamente do XR (policies já existentes)
	cachePolicyIdFromXR, _ := xr.Resource.GetString("spec.cachePolicyId")
	originRequestPolicyIdFromXR, _ := xr.Resource.GetString("spec.originRequestPolicyId")
	responseHeadersPolicyIdFromXR, _ := xr.Resource.GetString("spec.responseHeadersPolicyId")

	// Aliases (CloudFront espera []*string)
	dnsPointers := make([]*string, len(dns))
	for i := range dns {
		dnsPointers[i] = ptr.To(dns[i])
	}

	// allowedMethods -> []*string
	var allowedMethodsPtrs []*string
	for _, m := range allowedMethods {
		allowedMethodsPtrs = append(allowedMethodsPtrs, ptr.To(m))
	}

	// Desired + Observed composed
	desired, err := request.GetDesiredComposedResources(req)
	if err != nil {
		response.Fatal(rsp, errors.Wrapf(err, "cannot get desired resources from %T", req))
		return rsp, nil
	}
	observedComposed, err := request.GetObservedComposedResources(req)
	if err != nil {
		response.Fatal(rsp, errors.Wrap(err, "cannot get observed composed resources"))
		return rsp, nil
	}

	// log.Info("Debug Logging",
	// 	"desired", desired,
	// 	"xr", xr,
	// 	"observedComposed", observedComposed,
	// )

	// registrar tipos no Scheme
	_ = cloudfront.AddToScheme(composed.Scheme)
	_ = bucket.AddToScheme(composed.Scheme)

	// Bucket
	b := &bucket.Bucket{
		ObjectMeta: metav1.ObjectMeta{
			Name: xrName + "-bucket",
			Annotations: map[string]string{
				"crossplane.io/external-name":    bucketName,
				annotationCompositionResourceKey: string(resBucket),
			},
		},
		Spec: bucket.BucketSpec{
			ForProvider: bucket.BucketParameters{
				Region: &region,
				Tags: map[string]*string{
					"Name": ptr.To(bucketName),
				},
			},
		},
	}
	cdBucket, err := composed.From(b)
	if err != nil {
		response.Fatal(rsp, errors.Wrapf(err, "cannot convert %T to %T", b, cdBucket))
		return rsp, nil
	}
	desired[resBucket] = &resource.DesiredComposed{Resource: cdBucket}

	if !composedReady(observedComposed, resBucket) {
		if err := response.SetDesiredComposedResources(rsp, desired); err != nil {
			response.Fatal(rsp, errors.Wrapf(err, "cannot set desired composed resources in %T", rsp))
			return rsp, nil
		}

		response.ConditionFalse(rsp, "FunctionSuccess", "WaitingBucket").
			WithMessage("Waiting for S3 bucket to be ready before creating dependent resources").
			TargetComposite()

		log.Info("Bucket not ready yet, skipping remaining resources",
			"bucket", bucketName,
			"xr", xrName,
		)

		return rsp, nil
	}
	// OwnershipControls
	boc := &bucket.BucketOwnershipControls{
		ObjectMeta: metav1.ObjectMeta{
			Name: xrName + "-ownership",
			Annotations: map[string]string{
				"crossplane.io/external-name":    bucketName,
				annotationCompositionResourceKey: string(resBucketOwnershipControls),
			},
		},
		Spec: bucket.BucketOwnershipControlsSpec{
			ForProvider: bucket.BucketOwnershipControlsParameters{
				Bucket: &bucketName,
				Region: &region,
				Rule: []bucket.BucketOwnershipControlsRuleParameters{
					{
						ObjectOwnership: ptr.To("BucketOwnerEnforced"),
					},
				},
			},
		},
	}
	cdBucketOwnershipControls, err := composed.From(boc)
	if err != nil {
		response.Fatal(rsp, errors.Wrapf(err, "cannot convert BucketOwnershipControls to unstructured"))
		return rsp, nil
	}
	desired[resBucketOwnershipControls] = &resource.DesiredComposed{Resource: cdBucketOwnershipControls}

	// PublicAccessBlock
	bpab := &bucket.BucketPublicAccessBlock{
		ObjectMeta: metav1.ObjectMeta{
			Name: xrName + "-pab",
			Annotations: map[string]string{
				"crossplane.io/external-name":    bucketName,
				annotationCompositionResourceKey: string(resBucketPublicAccessBlock),
			},
		},
		Spec: bucket.BucketPublicAccessBlockSpec{
			ForProvider: bucket.BucketPublicAccessBlockParameters{
				Bucket:                &bucketName,
				Region:                &region,
				BlockPublicAcls:       ptr.To(true),
				BlockPublicPolicy:     ptr.To(true),
				IgnorePublicAcls:      ptr.To(true),
				RestrictPublicBuckets: ptr.To(true),
			},
		},
	}
	cdBucketPublicAccessBlock, err := composed.From(bpab)
	if err != nil {
		response.Fatal(rsp, errors.Wrapf(err, "cannot convert BucketPublicAccessBlock to unstructured"))
		return rsp, nil
	}
	desired[resBucketPublicAccessBlock] = &resource.DesiredComposed{Resource: cdBucketPublicAccessBlock}

	// CORS
	bcc := &bucket.BucketCorsConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: xrName + "-cors",
			Annotations: map[string]string{
				"crossplane.io/external-name":    bucketName,
				annotationCompositionResourceKey: string(resBucketCorsConfiguration),
			},
		},
		Spec: bucket.BucketCorsConfigurationSpec{
			ForProvider: bucket.BucketCorsConfigurationParameters{
				Bucket: &bucketName,
				Region: &region,
				CorsRule: []bucket.BucketCorsConfigurationCorsRuleParameters{
					{
						AllowedMethods: []*string{ptr.To("GET"), ptr.To("HEAD")},
						AllowedOrigins: []*string{ptr.To("*")},
						MaxAgeSeconds:  ptr.To(float64(3600)),
					},
				},
			},
		},
	}
	cdBucketCorsConfiguration, err := composed.From(bcc)
	if err != nil {
		response.Fatal(rsp, errors.Wrapf(err, "cannot convert BucketCorsConfiguration to unstructured"))
		return rsp, nil
	}
	desired[resBucketCorsConfiguration] = &resource.DesiredComposed{Resource: cdBucketCorsConfiguration}

	// Website
	bwc := &bucket.BucketWebsiteConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: xrName + "-website",
			Annotations: map[string]string{
				"crossplane.io/external-name":    bucketName,
				annotationCompositionResourceKey: string(resBucketWebsiteConfiguration),
			},
		},
		Spec: bucket.BucketWebsiteConfigurationSpec{
			ForProvider: bucket.BucketWebsiteConfigurationParameters{
				Bucket: &bucketName,
				Region: &region,
				IndexDocument: []bucket.IndexDocumentParameters{
					{
						Suffix: ptr.To("index.html"),
					},
				},
				RoutingRule: []bucket.RoutingRuleParameters{},
			},
		},
	}
	cdBucketWebsiteConfiguration, err := composed.From(bwc)
	if err != nil {
		response.Fatal(rsp, errors.Wrapf(err, "cannot convert BucketWebsiteConfiguration to unstructured"))
		return rsp, nil
	}
	desired[resBucketWebsiteConfiguration] = &resource.DesiredComposed{Resource: cdBucketWebsiteConfiguration}

	// SSE
	bssec := &bucket.BucketServerSideEncryptionConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: xrName + "-sse",
			Annotations: map[string]string{
				"crossplane.io/external-name":    bucketName,
				annotationCompositionResourceKey: string(resBucketSSEConfiguration),
			},
		},
		Spec: bucket.BucketServerSideEncryptionConfigurationSpec{
			ForProvider: bucket.BucketServerSideEncryptionConfigurationParameters{
				Bucket: &bucketName,
				Region: &region,
				Rule: []bucket.BucketServerSideEncryptionConfigurationRuleParameters{
					{
						ApplyServerSideEncryptionByDefault: []bucket.RuleApplyServerSideEncryptionByDefaultParameters{
							{
								SseAlgorithm: ptr.To("AES256"),
							},
						},
					},
				},
			},
		},
	}
	cdBucketSSEConfiguration, err := composed.From(bssec)
	if err != nil {
		log.Info("Error converting BucketServerSideEncryptionConfiguration to unstructured", "error", err)
		response.Fatal(rsp, errors.Wrapf(err, "cannot convert BucketServerSideEncryptionConfiguration to unstructured"))
		return rsp, nil
	}
	desired[resBucketSSEConfiguration] = &resource.DesiredComposed{Resource: cdBucketSSEConfiguration}

	// OAC
	oac := &cloudfront.OriginAccessControl{
		ObjectMeta: metav1.ObjectMeta{
			Name: xrName + "-oac",
			Annotations: map[string]string{
				// "crossplane.io/external-name":    bucketName + "-oac",
				annotationCompositionResourceKey: string(resOriginAccessControl),
			},
		},
		Spec: cloudfront.OriginAccessControlSpec{
			ForProvider: cloudfront.OriginAccessControlParameters{
				Name:                          ptr.To(bucketName + "-oac"),
				Description:                   ptr.To("Origin Access Control for " + bucketName),
				OriginAccessControlOriginType: ptr.To("s3"),
				SigningBehavior:               ptr.To("always"),
				SigningProtocol:               ptr.To("sigv4"),
			},
		},
	}
	cdOriginAccessControl, err := composed.From(oac)
	if err != nil {
		log.Info("Error converting OriginAccessControl to unstructured", "error", err)
		response.Fatal(rsp, errors.Wrapf(err, "cannot convert OriginAccessControl to unstructured"))
		return rsp, nil
	}
	desired[resOriginAccessControl] = &resource.DesiredComposed{Resource: cdOriginAccessControl}

	// IDs das policies que vamos usar na Distribution
	var (
		cachePolicyID           string
		originRequestPolicyID   string
		responseHeadersPolicyID string
	)

	// ResponseHeadersPolicy
	if createRHP {
		rhp := &cloudfront.ResponseHeadersPolicy{
			ObjectMeta: metav1.ObjectMeta{
				Name: xrName + "-rhp",
				Annotations: map[string]string{
					// "crossplane.io/external-name":    bucketName + "-rhp",
					annotationCompositionResourceKey: string(resResponseHeadersPolicy),
				},
			},
			Spec: cloudfront.ResponseHeadersPolicySpec{
				ForProvider: cloudfront.ResponseHeadersPolicyParameters{
					Name:    ptr.To(bucketName + "-rhp"),
					Comment: ptr.To("Response Headers Policy for " + bucketName),

					CorsConfig: []cloudfront.CorsConfigParameters{
						{
							AccessControlAllowCredentials: ptr.To(true),
							AccessControlAllowHeaders: []cloudfront.AccessControlAllowHeadersParameters{
								{Items: []*string{ptr.To("test")}},
							},
							AccessControlAllowMethods: []cloudfront.AccessControlAllowMethodsParameters{
								{Items: []*string{ptr.To("GET")}},
							},
							AccessControlAllowOrigins: []cloudfront.AccessControlAllowOriginsParameters{
								{Items: []*string{ptr.To("*")}},
							},
							OriginOverride: ptr.To(true),
						},
					},
					CustomHeadersConfig: []cloudfront.CustomHeadersConfigParameters{
						{
							Items: []cloudfront.CustomHeadersConfigItemsParameters{
								{
									Header:   ptr.To("teste"),
									Value:    ptr.To("teste"),
									Override: ptr.To(true),
								},
							},
						},
					},
				},
			},
		}
		cdRHP, err := composed.From(rhp)
		if err != nil {
			log.Info("Error converting ResponseHeadersPolicy to unstructured", "error", err)
			response.Fatal(rsp, errors.Wrapf(err, "cannot convert ResponseHeadersPolicy to unstructured"))
			return rsp, nil
		}
		desired[resResponseHeadersPolicy] = &resource.DesiredComposed{Resource: cdRHP}

	}

	// OriginRequestPolicy
	if createORP {
		orp := &cloudfront.OriginRequestPolicy{
			ObjectMeta: metav1.ObjectMeta{
				Name: xrName + "-orp",
				Annotations: map[string]string{
					// "crossplane.io/external-name":    bucketName + "-orp",
					annotationCompositionResourceKey: string(resOriginRequestPolicy),
				},
			},
			Spec: cloudfront.OriginRequestPolicySpec{
				ForProvider: cloudfront.OriginRequestPolicyParameters{
					Comment: ptr.To("Origin Request Policy for " + bucketName),
					HeadersConfig: []cloudfront.OriginRequestPolicyHeadersConfigParameters{
						{
							HeaderBehavior: ptr.To("allViewer"),
						},
					},
					CookiesConfig: []cloudfront.OriginRequestPolicyCookiesConfigParameters{
						{
							CookieBehavior: ptr.To("all"),
						},
					},
					QueryStringsConfig: []cloudfront.OriginRequestPolicyQueryStringsConfigParameters{
						{
							QueryStringBehavior: ptr.To("all"),
						},
					},
				},
			},
		}
		cdORP, err := composed.From(orp)
		if err != nil {
			log.Info("Error converting OriginRequestPolicy to unstructured", "error", err)
			response.Fatal(rsp, errors.Wrapf(err, "cannot convert OriginRequestPolicy to unstructured"))
			return rsp, nil
		}
		desired[resOriginRequestPolicy] = &resource.DesiredComposed{Resource: cdORP}
	}

	// CachePolicy
	if createCP {
		cp := &cloudfront.CachePolicy{
			ObjectMeta: metav1.ObjectMeta{
				Name: xrName + "-cp",
				Annotations: map[string]string{
					// "crossplane.io/external-name":    bucketName + "-cp",
					annotationCompositionResourceKey: string(resCachePolicy),
				},
			},
			Spec: cloudfront.CachePolicySpec{
				ForProvider: cloudfront.CachePolicyParameters{
					Name:       ptr.To(bucketName + "-cp"),
					Comment:    ptr.To("Cache Policy for " + bucketName),
					DefaultTTL: ptr.To(float64(86400)),
					MaxTTL:     ptr.To(float64(86400)),
					MinTTL:     ptr.To(float64(0)),
					ParametersInCacheKeyAndForwardedToOrigin: []cloudfront.ParametersInCacheKeyAndForwardedToOriginParameters{
						{
							HeadersConfig: []cloudfront.HeadersConfigParameters{
								{
									HeaderBehavior: ptr.To("none"),
								},
							},
							CookiesConfig: []cloudfront.CookiesConfigParameters{
								{
									CookieBehavior: ptr.To("none"),
								},
							},
							QueryStringsConfig: []cloudfront.QueryStringsConfigParameters{
								{
									QueryStringBehavior: ptr.To("none"),
								},
							},
						},
					},
				},
			},
		}
		cdCP, err := composed.From(cp)
		if err != nil {
			log.Info("Error converting CachePolicy to unstructured", "error", err)
			response.Fatal(rsp, errors.Wrapf(err, "cannot convert CachePolicy to unstructured"))
			return rsp, nil
		}
		desired[resCachePolicy] = &resource.DesiredComposed{Resource: cdCP}
	}

	var (
		cachePolicyIDPtr           *string
		originRequestPolicyIDPtr   *string
		responseHeadersPolicyIDPtr *string
	)
	if createRHP || createORP || createCP {
		missing := resolvePolicies(observedComposed, []policySpec{
			{
				name:        "cachePolicy",
				create:      createCP,
				fromXR:      cachePolicyIdFromXR,
				observedRes: resCachePolicy,
				outPtr:      &cachePolicyIDPtr,
			},
			{
				name:        "originRequestPolicy",
				create:      createORP,
				fromXR:      originRequestPolicyIdFromXR,
				observedRes: resOriginRequestPolicy,
				outPtr:      &originRequestPolicyIDPtr,
			},
			{
				name:        "responseHeadersPolicy",
				create:      createRHP,
				fromXR:      responseHeadersPolicyIdFromXR,
				observedRes: resResponseHeadersPolicy,
				outPtr:      &responseHeadersPolicyIDPtr,
			},
		})

		if len(missing) > 0 {
			if err := response.SetDesiredComposedResources(rsp, desired); err != nil {
				log.Info("Error setting desired composed resources", "error", err)
				response.Fatal(rsp, errors.Wrapf(err, "cannot set desired composed resources in %T", rsp))
				return rsp, nil
			}

			response.ConditionFalse(rsp, "FunctionSuccess", "WaitingPolicies").
				WithMessage(fmt.Sprintf("Waiting for CloudFront policy IDs: %v", missing)).
				TargetComposite()

			log.Info("Policies not ready yet, skipping Distribution",
				"missing", missing,
				"cachePolicyIDPtr", cachePolicyIDPtr,
				"originRequestPolicyIDPtr", originRequestPolicyIDPtr,
				"responseHeadersPolicyIDPtr", responseHeadersPolicyIDPtr,
				"xr", xrName,
			)

			return rsp, nil

		}
	}
	// Distribution
	distribution := &cloudfront.Distribution{
		ObjectMeta: metav1.ObjectMeta{
			Name: xrName + "-distribution",
			Annotations: map[string]string{
				// "crossplane.io/external-name":    bucketName,
				annotationCompositionResourceKey: string(resDistribution),
			},
		},
		Spec: cloudfront.DistributionSpec{
			ForProvider: cloudfront.DistributionParameters{
				Aliases:           dnsPointers,
				DefaultRootObject: &defaultRootObject,
				Comment:           &bucketName,
				HTTPVersion:       &httpVersion,
				PriceClass:        &priceClass,
				ViewerCertificate: []cloudfront.ViewerCertificateParameters{
					{
						CloudfrontDefaultCertificate: ptr.To(false),
						SSLSupportMethod:             ptr.To("sni-only"),
						MinimumProtocolVersion:       &minimumProtocolVersion,
						AcmCertificateArn:            &certificateArn,
					},
				},
				Origin: []cloudfront.OriginParameters{
					{
						DomainName: ptr.To(bucketName + ".s3." + region + ".amazonaws.com"),
						OriginID:   ptr.To("origin-bucket-" + bucketName),
						OriginAccessControlIDRef: &xpv1.Reference{
							Name: xrName + "-oac",
						},
					},
				},
				DefaultCacheBehavior: []cloudfront.DefaultCacheBehaviorParameters{
					{
						CachedMethods:           []*string{ptr.To("GET"), ptr.To("HEAD")},
						AllowedMethods:          allowedMethodsPtrs,
						TargetOriginID:          ptr.To("origin-bucket-" + bucketName),
						ViewerProtocolPolicy:    ptr.To("redirect-to-https"),
						Compress:                ptr.To(true),
						CachePolicyID:           cachePolicyIDPtr,
						OriginRequestPolicyID:   originRequestPolicyIDPtr,
						ResponseHeadersPolicyID: responseHeadersPolicyIDPtr,
					},
				},
				Enabled: ptr.To(true),
				Restrictions: []cloudfront.RestrictionsParameters{
					{
						GeoRestriction: []cloudfront.GeoRestrictionParameters{
							{
								RestrictionType: ptr.To("none"),
							},
						},
					},
				},
			},
		},
	}

	cdDistribution, err := composed.From(distribution)
	if err != nil {
		log.Info("Error converting Distribution to unstructured", "error", err)
		response.Fatal(rsp, errors.Wrapf(err, "cannot convert Distribution to unstructured"))
		return rsp, nil
	}
	desired[resDistribution] = &resource.DesiredComposed{Resource: cdDistribution}

	if !composedReady(observedComposed, resDistribution) {
		if err := response.SetDesiredComposedResources(rsp, desired); err != nil {
			response.Fatal(rsp, errors.Wrapf(err, "cannot set desired composed resources in %T", rsp))
			return rsp, nil
		}

		response.ConditionFalse(rsp, "FunctionSuccess", "WaitingDistribution").
			WithMessage("Waiting for CloudFront distribution to be ready before creating dependent resources").
			TargetComposite()

		log.Info("Distribution not ready yet, skipping remaining resources",
			"distribution", xrName+"-distribution",
			"xr", xrName,
		)

		return rsp, nil
	}

	id := isDistributionReady(observedComposed)
	// Policy (S3 + CloudFront)
	policyJSON := fmt.Sprintf(`{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::%[1]s/*",
      "Condition": {
        "StringLike": {
          "AWS:SourceArn": "arn:aws:cloudfront::%[2]s:distribution/%[3]s"
        }
      }
    },
    {
      "Effect": "Deny",
      "Principal": "*",
      "Action": "s3:*",
      "Resource": [
        "arn:aws:s3:::%[1]s/*",
        "arn:aws:s3:::%[1]s"
      ],
      "Condition": {
        "Bool": {
          "aws:SecureTransport": "false"
        }
      }
    }
  ]
}`, bucketName, awsAccountId, id)

	bp := &bucket.BucketPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: xrName + "-policy",
			Annotations: map[string]string{
				"crossplane.io/external-name":    bucketName,
				annotationCompositionResourceKey: string(resBucketPolicy),
			},
		},
		Spec: bucket.BucketPolicySpec{
			ForProvider: bucket.BucketPolicyParameters{
				Bucket: &bucketName,
				Region: &region,
				Policy: ptr.To(policyJSON),
			},
		},
	}
	cdBucketPolicy, err := composed.From(bp)
	if err != nil {
		log.Info("Error converting BucketPolicy to unstructured", "error", err)
		response.Fatal(rsp, errors.Wrapf(err, "cannot convert BucketPolicy to unstructured"))
		return rsp, nil
	}
	desired[resBucketPolicy] = &resource.DesiredComposed{Resource: cdBucketPolicy}

	// salvar desired
	if err := response.SetDesiredComposedResources(rsp, desired); err != nil {
		log.Info("Error setting desired composed resources", "error", err)
		response.Fatal(rsp, errors.Wrapf(err, "cannot set desired composed resources in %T", rsp))
		return rsp, nil
	}

	log.Info("Updated desired resources for Frontend",
		"bucket", bucketName,
		"distribution", xrName+"-distribution",
		"cachePolicyID", cachePolicyID,
		"originRequestPolicyID", originRequestPolicyID,
		"responseHeadersPolicyID", responseHeadersPolicyID,
	)

	response.ConditionTrue(rsp, "FunctionSuccess", "Success").
		WithMessage("Static frontend S3 + CloudFront composed successfully.").
		TargetComposite()

	return rsp, nil
}
